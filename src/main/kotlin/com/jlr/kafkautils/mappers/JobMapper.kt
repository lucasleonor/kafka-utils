package com.jlr.kafkautils.mappers

import com.jlr.kafkautils.jobs.Job
import com.jlr.kafkautils.jobs.parameters.ProvidedParameters

class JobMapper(val job: Job, val params: ProvidedParameters) {

}