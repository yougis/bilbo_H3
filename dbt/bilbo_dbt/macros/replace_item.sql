{% macro replace_item(list, item, pos) %}
    {% set len_list = list|length %}
    {% set list_copy = [] %}
    {% for elem in list %}
        {% do list_copy.append(elem) %}
    {% endfor %}
    {% for i in range(len_list) %}
        {% do list.pop() %}
    {% endfor %}
    {% for elem in list_copy %}
        {% if loop.index == pos+1 %}
            {% do list.append(item) %}
        {% else %}
            {% do list.append(elem) %}
        {% endif %}
    {% endfor %}
{% endmacro %}