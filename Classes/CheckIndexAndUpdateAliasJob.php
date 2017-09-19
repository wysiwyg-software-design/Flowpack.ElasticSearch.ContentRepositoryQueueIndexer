<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\ElasticSearch\Domain\Model\Index;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Error\Messages\Error;
use Neos\Error\Messages\Result;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Utility\Algorithms;

/**
 * ElasticSearch Indexing Job Interface
 */
class CheckIndexAndUpdateAliasJob implements JobInterface
{
    use LoggerTrait;

    /**
     * @var NodeIndexer
     * @Flow\Inject
     */
    protected $nodeIndexer;

    /**
     * @Flow\Inject
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    /**
     * @var string
     */
    protected $identifier;

    /**
     * @var string
     */
    protected $indexPostfix;

    /**
     * map of validator classnames and if they should be used. ['className' => bool]
     *
     * @var array
     */
    protected $validatorClassNames;

    /**
     * @param string $indexPostfix
     * @param array $validators;
     */
    public function __construct($indexPostfix, $validators)
    {
        $this->identifier = Algorithms::generateRandomString(24);
        $this->indexPostfix = $indexPostfix;
        $this->validatorClassNames = $validators;
    }

    /**
     * Execute the job
     * A job should finish itself after successful execution using the queue methods.
     *
     * @param QueueInterface $queue
     * @param Message $message The original message
     * @return boolean TRUE if the job was executed successfully and the message should be finished
     */
    public function execute(QueueInterface $queue, Message $message)
    {
        $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);

        $validators = $this->prepareValidators($this->validatorClassNames);
        $result = $this->checkIndexValidity($validators, $this->nodeIndexer->getIndex());

        if ($result->hasErrors()) {
            foreach ($result->getErrors() as $error) {
                $this->log($error->render(), LOG_ERR);
            }

            return false;
        }

        $this->nodeIndexer->updateIndexAlias();
        $this->log(sprintf('action=indexing step=index-switched alias=%s', $this->indexPostfix), LOG_NOTICE);

        return true;
    }

    /**
     * Get an optional identifier for the job
     *
     * @return string A job identifier
     */
    public function getIdentifier()
    {
        return $this->identifier;
    }

    /**
     * Get a readable label for the job
     *
     * @return string A label for the job
     */
    public function getLabel()
    {
        return sprintf('ElasticSearch Indexing Job (%s)', $this->getIdentifier());
    }

    /**
     * @param array $validatorClassNames
     * @return IndexValidatorInterface[]
     */
    protected function prepareValidators(array $validatorClassNames)
    {
        $validatorClassNames = array_keys(array_filter($validatorClassNames, function ($shouldBeUsed) {
            return ($shouldBeUsed === true);
        }));

        return array_map(function ($validatorClassName) {
            return $this->objectManager->get($validatorClassName);
        }, $validatorClassNames);
    }

    /**
     * @param array $indexValidators
     * @param Index $index
     * @return Result
     */
    protected function checkIndexValidity(array $indexValidators, Index $index)
    {
        $result = array_reduce($indexValidators, function (Result $result, IndexValidatorInterface $validator) use ($index) {
            $validationResult = $validator->isValid($index);
            $result->merge($validationResult);
            return $result;
        }, new Result());

        return $result;
    }
}
