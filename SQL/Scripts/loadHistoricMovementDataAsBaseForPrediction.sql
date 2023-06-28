WITH first_agg AS (
	SELECT 
		AVG(MVMT_USAGE) AS MVMT_USAGE, 
		MAX(PART_I_D) AS PART_I_D, 
		process_date, 
		mvmt_object_i_d 
	FROM PTE."pa_export" 
	WHERE 
        PART_I_D IN ('111776')
	    AND PROCESS_DATE  >= '2021-06-14'
	    AND PROCESS_DATE  <= '2023-03-16'
	GROUP BY 
		process_date,
		PART_I_D, 
		mvmt_object_i_d) 
SELECT 
	PROCESS_DATE, 
	COALESCE(SUM(MVMT_USAGE), 0) AS part_consumption, 
	PART_I_D 
FROM first_agg 
GROUP BY 
	PROCESS_DATE, 
	PART_I_D
ORDER BY 
    PROCESS_DATE 