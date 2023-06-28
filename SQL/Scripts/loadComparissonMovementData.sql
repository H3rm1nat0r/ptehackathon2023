WITH first_agg AS (
	SELECT 
		avg(mvmt_usage) AS mvmt_usage, 
		max(part_i_d) AS PART_I_D, 
		PROCESS_DATE, 
		mvmt_object_i_d 
	FROM PTE."pa_export" 
	WHERE 
        PART_I_D IN ('111776')
	    AND PROCESS_DATE  > '2023-03-16'
        AND PROCESS_DATE <= '2023-06-14'
	GROUP BY 
		PROCESS_DATE,
		PART_I_D, 
		mvmt_object_i_d) 
SELECT 
	PROCESS_DATE, 
	COALESCE(sum(mvmt_usage), 0) AS PART_CONSUMPTION, 
	PART_I_D 
FROM first_agg 
GROUP BY 
	PROCESS_DATE, 
	PART_I_D
ORDER BY 
    PROCESS_DATE 
;        