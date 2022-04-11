package com.jlr.kafkautils.jobs

import com.jlr.kafkautils.Kafka
import com.jlr.kafkautils.jobs.parameters.ExpectedParameter
import com.jlr.kafkautils.jobs.parameters.ExpectedParameters
import com.jlr.kafkautils.jobs.parameters.ProvidedParameters
import org.apache.kafka.clients.producer.ProducerRecord
import java.nio.file.Files
import kotlin.io.path.Path
import kotlin.io.path.isDirectory
import kotlin.io.path.isRegularFile

class Publish : Job(
    "publish", "Publishes messages to a specific topic", ExpectedParameters.of(
        ExpectedParameter("topic", "Topic to publish to"),
        ExpectedParameter(
            "message",
            "Message to be published. Either this or \"messagePath\" needs to be provided",
            required = false
        ),
        ExpectedParameter(
            "messagePath",
            "File or directory to publish, the file (or all files in that directory) will be read and published. Either this or \"message\" needs to be provided",
            required = false
        ),
        ExpectedParameter("repeat", "Number of times to repeat the messages", "1")
        )
) {

    override fun execute(params: ProvidedParameters) {
        val messagesToSend = getMessagesToSend(params)
        val topic = params["topic"]
        val producer = Kafka.producer()
        for (i in 1..params["repeat"].toInt()) {
            println("Publishing ${messagesToSend.size} messages to topic $topic - $i")
            messagesToSend.forEach {
                producer.send(ProducerRecord(topic, it)).get()
            }
        }
    }

    private fun getMessagesToSend(params: ProvidedParameters): List<String> {
        val message = params["message"]
        if (message.isNotBlank()) {
            return listOf(message)
        }
        val path = Path(params["messagePath"])
        if (path.isDirectory()) {
            return Files.walk(path)
                .filter { it.isRegularFile() }
                .map { Files.readString(it) }
                .toList()
        }
        return listOf(Files.readString(path))
    }

    companion object {
        fun customValidation(params: ProvidedParameters) {
            val providedParamNames = params.getNames()
            val containsMessage = providedParamNames.contains("message")
            val containsMessagePath = providedParamNames.contains("messagePath")
            if (containsMessage && containsMessagePath) {
                throw ValidationException(
                    "Both \"message\" and \"messagePath\" parameters were provided, only one of them should be provided"
                )
            }
            if (!containsMessage && !containsMessagePath) {
                throw ValidationException(
                    "Neither \"message\" or \"messagePath\" parameters were provided, one of them should be provided"
                )
            }
            if (params["repeat"].toInt() < 1) {
                throw ValidationException("Repeat needs to be 1 or bigger")
            }
        }
    }
}
