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

use pmmp\thread\ThreadSafe;
use pmmp\thread\ThreadSafeArray;
use function serialize;

class RequestTimingsQueue extends ThreadSafe{
	/** @var bool */
	private $invalidated = false;
	/** @var ThreadSafeArray */
	private $queries;

	const ENABLE_TIMINGS = 0;
	const RELOAD_TIMINGS = 1;
	const DISABLE_TIMINGS = 2;
	const GET_TIMINGS = 3;

	public function __construct(){
		$this->queries = new ThreadSafeArray();
	}

	public function scheduleTimings(int $queryId, int $action) : void{
		if($this->invalidated){
			throw new QueueShutdownException("You cannot schedule a query on an invalidated queue.");
		}
		$this->synchronized(function() use ($queryId, $action) : void{
			$this->queries[] = serialize([$queryId, $action]);
			$this->notifyOne();
		});
	}

	public function fetchTimings() : ?string {
		return $this->synchronized(function(): ?string {
			while($this->queries->count() === 0 && !$this->isInvalidated()){
				$this->wait();
			}
			return $this->queries->shift();
		});
	}

	public function invalidate() : void {
		$this->synchronized(function():void{
			$this->invalidated = true;
			$this->notify();
		});
	}

	/**
	 * @return bool
	 */
	public function isInvalidated(): bool {
		return $this->invalidated;
	}

	public function count() : int{
		return $this->queries->count();
	}
}
