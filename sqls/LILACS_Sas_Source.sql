SELECT
        'LILACS_Sas_Source' as dbase,
        CONCAT_WS('-','fiadmin',a.`id`) AS id,
        b.title_serial,
        b.issn,
        b.volume_serial,
        b.issue_number,
        LEFT(a.publication_date_normalized, 4) as publication_year
FROM `biblioref_referencesource` AS b
INNER JOIN `biblioref_reference` AS a
ON a.`id` = b.`reference_ptr_id` and a.literature_type like 'S%' and a.treatment_level = '';
