package com.jlr.kafkautils.jobs

import com.jlr.kafkautils.Kafka
import com.jlr.kafkautils.jobs.parameters.ProvidedParameters

class ListTopics : Job("listTopics", "Lists all topics present in the brokers") {
    private val consumer = Kafka.consumer()

    override fun execute(params: ProvidedParameters) {
        consumer.listTopics().forEach { println(it.key) }
    }
}