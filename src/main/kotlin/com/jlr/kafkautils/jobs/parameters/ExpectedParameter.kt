package com.jlr.kafkautils.jobs.parameters

class ExpectedParameter(
    val name: String,
    private val description: String,
    val defaultValue: String? = null,
    val required: Boolean = defaultValue == null
) {
    fun getDocumentation(): String {
        val maxLength = 60
        val lines = ArrayList<String>()
        if (description.length <= maxLength) {
            lines.add(description)
        } else {
            val words = description.split(" ")
            var line = ""
            for (word in words) {
                val wordSpace = "$word "
                if (line.length + word.length > maxLength) {
                    lines.add(line)
                    line = wordSpace
                } else {
                    line += wordSpace
                }
            }
            lines.add(line)
        }
        val result = StringBuilder(
            String.format(
                "%n\t  %-20s %-61s %-20s %s", name, lines.removeAt(0), if (required) "yes" else "no",
                defaultValue ?: ""
            )
        )
        lines.forEach {
            result.append(String.format("%n\t%-22s %-60s", "", it))
        }
        return result.toString()
    }
}