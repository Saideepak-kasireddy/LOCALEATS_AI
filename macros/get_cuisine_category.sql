{% macro get_cuisine_category(cuisine_field) %}
    CASE
        WHEN LOWER({{ cuisine_field }}) LIKE '%italian%' THEN 'Italian'
        WHEN LOWER({{ cuisine_field }}) LIKE '%chinese%' THEN 'Chinese'
        WHEN LOWER({{ cuisine_field }}) LIKE '%mexican%' THEN 'Mexican'
        WHEN LOWER({{ cuisine_field }}) LIKE '%japanese%' OR LOWER({{ cuisine_field }}) LIKE '%sushi%' THEN 'Japanese'
        WHEN LOWER({{ cuisine_field }}) LIKE '%thai%' THEN 'Thai'
        WHEN LOWER({{ cuisine_field }}) LIKE '%indian%' THEN 'Indian'
        WHEN LOWER({{ cuisine_field }}) LIKE '%american%' OR LOWER({{ cuisine_field }}) LIKE '%burger%' THEN 'American'
        WHEN LOWER({{ cuisine_field }}) LIKE '%pizza%' THEN 'Pizza'
        WHEN LOWER({{ cuisine_field }}) LIKE '%vietnamese%' OR LOWER({{ cuisine_field }}) LIKE '%pho%' THEN 'Vietnamese'
        WHEN LOWER({{ cuisine_field }}) LIKE '%korean%' THEN 'Korean'
        WHEN LOWER({{ cuisine_field }}) LIKE '%mediterranean%' OR LOWER({{ cuisine_field }}) LIKE '%greek%' THEN 'Mediterranean'
        ELSE 'Other'
    END
{% endmacro %}