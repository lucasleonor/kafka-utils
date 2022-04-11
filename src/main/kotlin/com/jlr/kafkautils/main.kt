package com.jlr.kafkautils

import com.jlr.kafkautils.jobs.Job
import com.jlr.kafkautils.jobs.JobException
import com.jlr.kafkautils.jobs.ValidationException
import com.jlr.kafkautils.jobs.parameters.ProvidedParameters
import org.apache.kafka.common.errors.TimeoutException
import java.util.concurrent.ExecutionException

fun main(args: Array<String>) {
    val arguments = args.toMutableList()

    val help = arguments.remove("--help")
    if (arguments.isEmpty()) {
        println(Job.getHelp())
        return
    }

    val jobName = arguments.removeAt(0)
    val job: Job? = Job.getJob(jobName)
    if (job == null) {
        println("Invalid job '$jobName'")
        println(Job.getHelp())
        return
    }
    if (help) {
        println(job.getDocumentation())
    }
    val parametersBuilder = ProvidedParameters.Builder(job.expectedParameters)
    arguments.forEach {
        val split = it.split("=")
        if (split.size != 2) {
            println("Invalid argument $it")
            return
        }
        parametersBuilder[split[0]] = split[1]
    }

    try {
        job.run(parametersBuilder.build())
    } catch (e: JobException) {
        println(e.message)
        if (e is ValidationException) println(job.getDocumentation())
    } catch (e: TimeoutException) {
        println("Couldn't connect to Kafka")
    } catch (e: ExecutionException) {
        if (e.cause is TimeoutException) {
            println("Couldn't connect to Kafka")
        } else {
            throw e
        }
    }

}