package com.jlr.kafkautils.jobs

open class JobException(msg: String) : Exception(msg)
class ValidationException(msg: String) : JobException(msg)
class TopicNotFound(topicName: String) : JobException("Topic '$topicName' not found")