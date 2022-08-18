{% macro replace_item(list, item, pos) %}
{#- Macro permettant de remplacer la valeur d-un item situé à la position pos. dans une liste -#}

    {#- Copie de la liste -#}
    {% set len_list = list|length %}
    {% set list_copy = [] %}
    {% for elem in list %}
        {% do list_copy.append(elem) %}
    {% endfor %}

    {#- Suppression de tous les éléments de la liste de départ -#}
    {% for i in range(len_list) %}
        {% do list.pop() %}
    {% endfor %}

    {#- Repeuplement de la liste -#}
    {% for elem in list_copy %}
        {% if loop.index == pos+1 %}
            {% do list.append(item) %}
        {% else %}
            {% do list.append(elem) %}
        {% endif %}
    {% endfor %}
    
{% endmacro %}