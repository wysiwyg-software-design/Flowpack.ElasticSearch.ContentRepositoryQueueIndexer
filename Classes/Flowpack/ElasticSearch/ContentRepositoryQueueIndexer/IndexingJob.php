<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer;

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\NodeIndexer;
use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Utility\Algorithms;
use TYPO3\TYPO3CR\Domain\Factory\NodeFactory;
use TYPO3\TYPO3CR\Domain\Model\NodeData;
use TYPO3\TYPO3CR\Domain\Model\NodeInterface;
use TYPO3\TYPO3CR\Domain\Repository\NodeDataRepository;
use TYPO3\TYPO3CR\Domain\Service\ContextFactory;

/**
 * ElasticSearch Indexing Job Interface
 */
class IndexingJob implements JobInterface
{
    use LoggerTrait;

    /**
     * @var NodeIndexer
     * @Flow\Inject
     */
    protected $nodeIndexer;

    /**
     * @var NodeDataRepository
     * @Flow\Inject
     */
    protected $nodeDataRepository;

    /**
     * @var NodeFactory
     * @Flow\Inject
     */
    protected $nodeFactory;

    /**
     * @var ContextFactory
     * @Flow\Inject
     */
    protected $contextFactory;

    /**
     * @var string
     */
    protected $identifier;

    /**
     * @var string
     */
    protected $workspaceName;

    /**
     * @var string
     */
    protected $indexPostfix;

    /**
     * @var array
     */
    protected $nodes = [];

    /**
     * @param string $indexPostfix
     * @param string $workspaceName
     * @param array $nodes
     */
    public function __construct($indexPostfix, $workspaceName, array $nodes)
    {
        $this->identifier = Algorithms::generateRandomString(24);
        $this->workspaceName = $workspaceName;
        $this->indexPostfix = $indexPostfix;
        $this->nodes = $nodes;
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
        $this->nodeIndexer->withBulkProcessing(function () {
            $numberOfNodes = count($this->nodes);
            $startTime = microtime(true);
            foreach ($this->nodes as $node) {
                /** @var NodeData $nodeData */
                $nodeData = $this->nodeDataRepository->findByIdentifier($node['nodeIdentifier']);
                $context = $this->contextFactory->create([
                    'workspaceName' => $this->workspaceName,
                    'invisibleContentShown' => true,
                    'inaccessibleContentShown' => false,
                    'dimensions' => $node['dimensions']
                ]);
                $currentNode = $this->nodeFactory->createFromNodeData($nodeData, $context);

                // Skip this iteration if the node can not be fetched from the current context
                if (!$currentNode instanceof NodeInterface) {
                    $this->log(sprintf('action=indexing step=failed node=%s message="Node could not be processed"', $node['nodeIdentifier']));
                    continue;
                }

                $this->nodeIndexer->setIndexNamePostfix($this->indexPostfix);
                $this->log(sprintf('action=indexing step=started node=%s', $currentNode->getIdentifier()));

                $this->nodeIndexer->indexNode($currentNode);
            }

            $this->nodeIndexer->flush();
            $duration = microtime(true) - $startTime;
            $rate = $numberOfNodes / $duration;
            $this->log(sprintf('action=indexing step=finished number_of_nodes=%d duration=%f nodes_per_second=%f', $numberOfNodes, $duration, $rate));
        });

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
}
