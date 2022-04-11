package com.jlr.kafkautils.jobs.parameters

class ProvidedParameters private constructor(
    private val parameters: Map<String, String>,
    private val expectedParameters: ExpectedParameters
) {

    fun getNames(): MutableSet<String> {
        return parameters.keys.toMutableSet()
    }

    operator fun get(name: String): String {
        return parameters.getOrDefault(name, expectedParameters[name].defaultValue.orEmpty())
    }

    class Builder(
        private val expectedParameters: ExpectedParameters,
    ) {
        private val parameters: MutableMap<String, String> = HashMap()

        operator fun set(name: String, value: String) = apply {
            parameters[name] = value
        }

        fun build() = ProvidedParameters(parameters, expectedParameters)
    }
}