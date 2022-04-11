package com.jlr.kafkautils.jobs

import com.jlr.kafkautils.Kafka
import com.jlr.kafkautils.jobs.parameters.ExpectedParameter
import com.jlr.kafkautils.jobs.parameters.ExpectedParameters
import com.jlr.kafkautils.jobs.parameters.ProvidedParameters
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import java.util.concurrent.ExecutionException

class CreateTopics : Job(
    "createTopics", "Creates the specified topics", ExpectedParameters.of(
        ExpectedParameter("topics", "List of topics to create"),
        ExpectedParameter("partitions", "Number of partitions to create for the topics", "1"),
        ExpectedParameter("replicationFactor", "Replication factor for the topics", "3"),
    )
) {

    override fun execute(params: ProvidedParameters) {
        val adminClient = Kafka.adminClient()

        val partitions = params["partitions"].toInt()
        val replicationFactor = params["replicationFactor"].toShort()
        val topicNames = params["topics"].split(",")
        val topics = topicNames.map { NewTopic(it, partitions, replicationFactor) }
        try {
            adminClient.createTopics(topics).all().get()
            println("Topics $topicNames created successfully")
        } catch (e: ExecutionException) {
            val cause = e.cause
            if (cause is TopicExistsException) {
                println(cause.message)
            } else {
                throw e
            }
        }
    }
}
