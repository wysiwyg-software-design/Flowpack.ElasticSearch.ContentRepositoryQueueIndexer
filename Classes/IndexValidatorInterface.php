<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

use Flowpack\ElasticSearch\Domain\Model\Index;
use Neos\Error\Messages\Result;

/**
 *
 *
 */
interface IndexValidatorInterface
{
    /**
     * @param Index $index
     * @return Result
     */
    public function  isValid(Index $index) : Result;
}
