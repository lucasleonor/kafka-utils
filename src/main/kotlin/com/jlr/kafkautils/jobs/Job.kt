package com.jlr.kafkautils.jobs

import com.jlr.kafkautils.jobs.parameters.ExpectedParameters
import com.jlr.kafkautils.jobs.parameters.ProvidedParameters

sealed class Job(
    val name: String,
    private val description: String,
    val expectedParameters: ExpectedParameters = ExpectedParameters()
) {

    fun getDocumentation(): String {
        return "\n $name:\t $description\n${expectedParameters.getDocumentation()}"
    }

    fun run(params: ProvidedParameters) {
        execute(params)
    }

    protected abstract fun execute(params: ProvidedParameters)

    companion object {
        private val availableJobs = listOf(Consume(), ListTopics(), Publish(), CreateTopics())

        fun getJob(name: String): Job? {
            val filteredJobs = availableJobs.filter { it.name == name }
            if (filteredJobs.isEmpty()) {
                return null
            }
            return filteredJobs[0]
        }

        fun getHelp(): String {
            var help = "Available Jobs:"
            availableJobs.forEach { help += "%n  %-15s %s".format(it.name, it.description) }
            return help
        }
    }

}
