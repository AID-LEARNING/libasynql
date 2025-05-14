<?php

/*
 * libasynql
 *
 * Copyright (C) 2018 SOFe
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

declare(strict_types=1);

namespace poggit\libasynql\base;

use InvalidArgumentException;
use pmmp\thread\Thread as NativeThread;
use pocketmine\GarbageCollectorManager;
use pocketmine\promise\Promise;
use pocketmine\promise\PromiseResolver;
use pocketmine\Server;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\thread\log\AttachableThreadSafeLogger;
use pocketmine\thread\Thread;
use pocketmine\timings\Timings;
use pocketmine\timings\TimingsHandler;
use poggit\libasynql\libasynql;
use poggit\libasynql\SqlError;
use poggit\libasynql\SqlResult;
use poggit\libasynql\SqlThread;
use poggit\libasynql\utils\TimingsModded;

abstract class SqlSlaveThread extends Thread implements SqlThread{
	private SleeperHandlerEntry $sleeperEntry;

	private static $nextSlaveNumber = 0;

	protected $slaveNumber;
	protected $bufferSend;
	protected $bufferRecv;
	protected $connCreated = false;
	protected $connError;
	protected $busy = false;
	protected AttachableThreadSafeLogger $logger;
	private RequestTimingsQueue $timingsRequest;
	private ResponseTimingsQueue $timingsResponse;

	private static ?GarbageCollectorManager $cycleGcManager = null;

	protected function __construct(SleeperHandlerEntry $entry, QuerySendQueue $bufferSend = null, QueryRecvQueue $bufferRecv = null){

		$this->sleeperEntry = $entry;
		$this->logger = Server::getInstance()->getLogger();

		$this->slaveNumber = self::$nextSlaveNumber++;
		$this->timingsRequest = new RequestTimingsQueue();
		$this->timingsResponse = new ResponseTimingsQueue();
		$this->bufferSend = $bufferSend ?? new QuerySendQueue();
		$this->bufferRecv = $bufferRecv ?? new QueryRecvQueue();

		if(!libasynql::isPackaged()){
			/** @noinspection PhpUndefinedMethodInspection */
			/** @noinspection NullPointerExceptionInspection */
			/** @var ClassLoader $cl */
			$cl = Server::getInstance()->getPluginManager()->getPlugin("DEVirion")->getVirionClassLoader();
			$this->setClassLoaders([Server::getInstance()->getLoader(), $cl]);
		}
		$this->start(NativeThread::INHERIT_INI);
	}


	public function addRequestTimings(int $timingsId, int $mode): void{
		$this->timingsRequest->scheduleTimings($timingsId, $mode);
	}

	protected function onRun() : void{
		\GlobalLogger::set($this->logger);
		$error = $this->createConn($resource);
		$this->connCreated = true;
		$this->connError = $error;

		$notifier = $this->sleeperEntry->createNotifier();

		if($error !== null){
			return;
		}
		Timings::init();
		self::$cycleGcManager = new GarbageCollectorManager($this->logger, Timings::$asyncTaskWorkers);
		$timing = null;
		$enableTiming = TimingsHandler::isEnabled();
		while(true){
			$timings = $this->timingsRequest->fetchTimings();
			if ($timings !== null){
				[$queryId, $action] = unserialize($timings);
				if($action === RequestTimingsQueue::ENABLE_TIMINGS){
					$enableTiming = true;
					TimingsHandler::setEnabled();
					\GlobalLogger::get()->debug("Enabled timings");
				}elseif($action === RequestTimingsQueue::DISABLE_TIMINGS){
					TimingsHandler::setEnabled(false);
					$enableTiming = false;
					\GlobalLogger::get()->debug("Disabled timings");
				}elseif($action === RequestTimingsQueue::RELOAD_TIMINGS) {
					TimingsHandler::reload();
					\GlobalLogger::get()->debug("Reset timings");
				}else if($action === RequestTimingsQueue::GET_TIMINGS) {
					$this->timingsResponse->publishResult($queryId, TimingsHandler::printCurrentThreadRecords());
					$notifier->wakeupSleeper();
				}
			}
			$row = $this->bufferSend->fetchQuery();
			if($row === null){
				break;
			}else if($row === false) {
				continue;
			}
			if ($enableTiming){
				$timing = TimingsModded::getInstance()->getCustomThreadRunTimings($this);
				$timing->startTiming();
			}
			$this->busy = true;
			[$queryId, $modes, $queries, $params] = unserialize($row, ["allowed_classes" => true]);
			try{
				$results = [];
				foreach($queries as $index => $query){
					$results[] = $this->executeQuery($resource, $modes[$index], $query, $params[$index]);
				}
				$this->bufferRecv->publishResult($queryId, $results);
			}catch(SqlError $error){
				$this->bufferRecv->publishError($queryId, $error);
			}finally{
				if ($enableTiming) {
					$timing->stopTiming();
				}
			}

			$notifier->wakeupSleeper();
			self::$cycleGcManager->maybeCollectCycles();
			$this->busy = false;
		}
		$this->close($resource);
	}

	/**
	 * @return bool
	 */
	public function isBusy() : bool{
		return $this->busy;
	}

	public function stopRunning() : void{
		$this->bufferSend->invalidate();

		parent::quit();
	}

	public function quit() : void{
		$this->stopRunning();
		parent::quit();
	}

	public function addQuery(int $queryId, array $modes, array $queries, array $params) : void{
		$this->bufferSend->scheduleQuery($queryId, $modes, $queries, $params);
	}

	public function readResults(array &$callbacks, ?int $expectedResults) : void{
		if($expectedResults === null){
			$resultsList = $this->bufferRecv->fetchAllResults();
		}else{
			$resultsList = $this->bufferRecv->waitForResults($expectedResults);
		}
		foreach($resultsList as [$queryId, $results]){
			if(!isset($callbacks[$queryId])){
				throw new InvalidArgumentException("Missing handler for query #$queryId");
			}

			$callbacks[$queryId]($results);
			unset($callbacks[$queryId]);
		}
	}

	/**
	 * @param array<PromiseResolver> $callbacks
	 * @param int|null $expectedResults
	 * @return void
	 */
	public function readResultsTimings(array &$callbacks, ?int $expectedResults) : void
	{
		if($expectedResults === null){
			$resultsList = $this->timingsResponse->fetchAllResults();
		}else{
			$resultsList = $this->timingsResponse->waitForResults($expectedResults);
		}
		foreach($resultsList as [$queryId, $results]){
			if(!isset($callbacks[$queryId])){
				throw new InvalidArgumentException("Missing handler for query #$queryId");
			}
			$callbacks[$queryId]->resolve($results);
			unset($callbacks[$queryId]);
		}
	}

	public function connCreated() : bool{
		return $this->connCreated;
	}

	public function hasConnError() : bool{
		return $this->connError !== null;
	}

	public function getConnError() : ?string{
		return $this->connError;
	}

	protected abstract function createConn(&$resource) : ?string;

	/**
	 * @param mixed   $resource
	 * @param int     $mode
	 * @param string  $query
	 * @param mixed[] $params
	 *
	 * @return SqlResult
	 * @throws SqlError
	 */
	protected abstract function executeQuery($resource, int $mode, string $query, array $params) : SqlResult;


	protected abstract function close(&$resource) : void;
}
