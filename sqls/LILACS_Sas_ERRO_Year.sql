SELECT 'LILACS_Sas' AS dbase,
       a.`id`,
       b.`title`,
       c.`title_serial`,
       SUBSTRING(a.`publication_date_normalized`,1,4) AS publication_year,
       c.`volume_serial`,
       c.`issue_number`,
       IFNULL(b.`corporate_author`,b.`individual_author`) AS author,
       b.`pages`
FROM `biblioref_reference` AS a,
     `biblioref_referenceanalytic` AS b,
     `biblioref_referencesource` as c
WHERE left(a.`literature_type`,1) = 'S'
AND a.`id` = b.`reference_ptr_id`
and b.`source_id` = c.`reference_ptr_id`
and a.`treatment_level` = 'as';
