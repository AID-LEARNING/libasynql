<?php

namespace poggit\libasynql\utils;

use pocketmine\scheduler\AsyncTask;
use pocketmine\timings\Timings;
use pocketmine\timings\TimingsHandler;
use pocketmine\utils\SingletonTrait;
use poggit\libasynql\base\SqlSlaveThread;

class TimingsModded
{
	use SingletonTrait;

	private \ReflectionProperty $asyncTaskRunProperty;
	public function __construct() {
		$this->asyncTaskRunProperty = new \ReflectionProperty(Timings::class, 'asyncTaskRun');
	}

	public function getCustomThreadRunTimings(SqlSlaveThread $thread, string $group = Timings::GROUP_MINECRAFT) : TimingsHandler{
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
}