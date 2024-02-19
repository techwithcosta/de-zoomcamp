{#
    This macro returns the description of the payment_type 
#}

{% macro float_to_integer(payment_type) -%}

    case cast( {{ payment_type }} as integer)
        when '1.0' then 1
        when '2.0' then 2
        when '3.0' then 3
        when '4.0' then 4
        when '5.0' then 5
        when '6.0' then 6
    end

{%- endmacro %}