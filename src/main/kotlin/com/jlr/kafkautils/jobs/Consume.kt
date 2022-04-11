package com.jlr.kafkautils.jobs

import com.jlr.kafkautils.Kafka
import com.jlr.kafkautils.jobs.parameters.ExpectedParameter
import com.jlr.kafkautils.jobs.parameters.ExpectedParameters
import com.jlr.kafkautils.jobs.parameters.ProvidedParameters
import java.lang.System.currentTimeMillis
import java.time.Duration

class Consume : Job(
    "consume", "Consumes messages from a specific topic", ExpectedParameters.of(
        ExpectedParameter("topic", "Topic to consume from"),
        ExpectedParameter("groupId", "groupId to use", "pipeline-utils"),
        ExpectedParameter(
            "timeout",
            "Timeout in seconds, if no new message is received for this many seconds the job will finish. " +
                    "To disable this set the value to 0",
            "60"
        ),
    )
) {
    private val pollTimeout = Duration.ofSeconds(1)

    override fun execute(params: ProvidedParameters) {
        val topicName = params["topic"]
        val consumer = Kafka.consumer(params["groupId"])
        if (!consumer.listTopics().containsKey(topicName)) {
            throw TopicNotFound(topicName)
        }
        consumer.subscribe(listOf(topicName))

        val timeout = params["timeout"].toLong()
        var count = 0
        var lastMessage = getCurrentTimeInSeconds()
        var timeToTimeout = timeout
        while (timeout <= 0 || timeToTimeout > 0) {
            consumer.poll(pollTimeout).forEach {
                lastMessage = getCurrentTimeInSeconds()
                println("${++count} - ${it.timestamp()} - ${it.value()}")
            }
            consumer.commitAsync()
            if (timeout > 0) {
                timeToTimeout = timeout - (getCurrentTimeInSeconds() - lastMessage)
            }
        }
    }

    private fun getCurrentTimeInSeconds(): Long {
        return currentTimeMillis() / 1000
    }
}