<?php
namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Validator;

use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexValidatorInterface;
use Flowpack\ElasticSearch\Domain\Model\Index;
use Neos\Error\Messages\Error;
use Neos\Error\Messages\Result;
use Neos\Flow\Annotations as Flow;

/**
 *
 */
class IndexSizeValidator implements IndexValidatorInterface
{
    const MINIMUM_DOCS_ALLOWED = 400000;

    /**
     * @Flow\InjectConfiguration()
     * @var int
     */
    protected $minimumAllowedDocuments;

    /**
     * Check if the index contains the minimal count of docs.
     *
     * @param Index $index
     * @return Result
     */
    public function isValid(Index $index): Result
    {
        $result = new Result();
        $httpResponse = $index->request('GET', '/_stats');
        if ($httpResponse->getStatusCode() !== 200) {
            $result->addError(new Error('Index responded with non 200 status code: ' . $httpResponse->getStatusCode()));
            return $result;
        }

        $responseContent = $httpResponse->getTreatedContent();
        if (!isset($responseContent['_all']['primaries']['docs']['count'])) {
            $result->addError(new Error('Index response did not contain docs count.'));
            return $result;
        }

        if ($responseContent['_all']['primaries']['docs']['count'] < self::MINIMUM_DOCS_ALLOWED) {
            $result->addError(new Error('Index contains less than minimum docs allowed: ' . $responseContent['_all']['primaries']['docs']['count']));
            return $result;
        }

        return $result;
    }
}
