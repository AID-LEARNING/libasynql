<?php

namespace poggit\libasynql\utils;

use pocketmine\scheduler\AsyncTask;
use pocketmine\timings\Timings;
use pocketmine\timings\TimingsHandler;
use poggit\libasynql\base\SqlSlaveThread;

class TimingsModded
{
	private static $instance;

	private \ReflectionProperty $asyncTaskRunProperty;
	private function __construct() {
		self::$instance = $this;
		$this->asyncTaskRunProperty = new \ReflectionProperty(Timings::class, 'asyncTaskRun');
	}

	public  function getCustomThreadRunTimings(SqlSlaveThread $thread, string $group = Timings::GROUP_MINECRAFT) : TimingsHandler{
		$taskClass = $thread::class;
		$asyncTaskRunProperty = $this->asyncTaskRunProperty;
		$asyncTaskRun = $asyncTaskRunProperty->getValue();
		if(!isset($asyncTaskRun[$taskClass])){
			Timings::init();
			$asyncTaskRun[$taskClass] = new TimingsHandler(
				"CustomThread - " . self::shortenCoreClassName($taskClass, "pocketmine\\") . " - Run",
				Timings::$asyncTaskWorkers,
				$group
			);
			$this->asyncTaskRunProperty->setValue(null, $asyncTaskRun);
		}

		return $asyncTaskRun[$taskClass];
	}

	private static function shortenCoreClassName(string $class, string $prefix) : string{
		if(str_starts_with($class, $prefix)){
			return (new \ReflectionClass($class))->getShortName();
		}
		return $class;
	}

	/**
	 * @return self
	 */
	public static function getInstance(): TimingsModded
	{
		return self::$instance;
	}
}