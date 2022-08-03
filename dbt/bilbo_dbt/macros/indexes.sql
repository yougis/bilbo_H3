{%- macro indexes(list, elem) -%}
    {%- set list_out = [] -%}
    {%- for i in range(list|length) -%}
        {%- if list[i] == elem -%}
            {%- do list_out.append(i) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(list_out) }}
{%- endmacro -%}