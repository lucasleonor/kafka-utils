package com.jlr.kafkautils.jobs.parameters

import com.jlr.kafkautils.jobs.ValidationException
import kotlin.reflect.KFunction1

class ExpectedParameters(
    private val parameters: List<ExpectedParameter> = emptyList(),
    private val extraValidation: (param: List<ProvidedParameters>) -> Unit = {}
) {

    operator fun get(name: String): ExpectedParameter {
        try {
            return parameters.first { it.name == name }
        } catch (e: NoSuchElementException) {
            throw ValidationException("Invalid param $name")
        }
    }

    fun validate(params: ProvidedParameters) {
        val providedNames = params.getNames()
        val notProvidedRequiredNames = parameters
            .filter { it.required }
            .map { it.name }
            .filter { !providedNames.contains(it) }
        if (notProvidedRequiredNames.isNotEmpty()) {
            throw ValidationException("Missing required parameter(s): $notProvidedRequiredNames")
        }
    }

    fun getDocumentation(): String {
        val builder = StringBuilder()
        if (parameters.isEmpty()) {
            builder.append("\tThis job doesn't accept any arguments")
        } else {
            builder.append(
                String.format("\t%-${20 + 2}s %-61s %-20s %s", "Argument", "Description", "Required", "Default Value")
            )
            for (parameter in parameters) {
                builder.append(parameter.getDocumentation())
            }
        }
        return builder.toString()
    }

    companion object {
        fun of(vararg param: ExpectedParameter): ExpectedParameters {
            return ExpectedParameters(param.toList())
        }
    }
}
