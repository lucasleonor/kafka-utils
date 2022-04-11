package com.jlr.kafkautils

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*

class Kafka {
    companion object {
        private fun getProps(): Properties {
            return Kafka::class.java.classLoader.getResourceAsStream("application.properties").use {
                Properties().apply { load(it) }
            }
        }

        fun consumer(groupId: String = "pipeline-utils"): KafkaConsumer<String, String> {
            val properties = getProps()
            properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
            return KafkaConsumer(properties)
        }

        fun producer(): KafkaProducer<String, String> {
            val properties = getProps()
            return KafkaProducer(properties)
        }

        fun adminClient(): AdminClient {
            val properties = getProps()
            return AdminClient.create(properties)
        }
    }
}
