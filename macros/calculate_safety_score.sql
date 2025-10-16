{% macro calculate_safety_score(pass_count, fail_count, violation_score, days_since) %}
    GREATEST(0, LEAST(100,
        60 +
        ({{ pass_count }} * 10) -
        ({{ fail_count }} * 15) -
        ({{ violation_score }} * 0.5) -
        (GREATEST(0, {{ days_since }} - 180) * 0.05)
    ))
{% endmacro %}