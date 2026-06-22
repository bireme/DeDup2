SELECT 
       'LILACS_MNTam' AS dbase,
       a.`id`,
       b.`english_translated_title` AS title,
       c.`title_monographic`,
       SUBSTRING(a.`publication_date_normalized`,1,4) AS publication_year,
       IFNULL(b.`individual_author`, b.`corporate_author`) AS author,
       b.`pages`,
       c.`volume_monographic`,
       c.`issue_number`,
       a.`cooperative_center_code`,
       a.`literature_type`,
       a.`treatment_level`,
       a.`status`,
       CASE 
		WHEN a.`electronic_address` IS NULL THEN ''
		WHEN a.`electronic_address` = '[]' THEN ''
		ELSE a.`electronic_address`
       END AS electronic_address,
       CONCAT('https://fi-admin.bvsalud.org/bibliographic/edit-analytic/', b.`reference_ptr_id`) AS link_fiadmin
FROM `biblioref_reference` AS a
JOIN `biblioref_referenceanalytic` AS b ON a.`id` = b.`reference_ptr_id`
JOIN `biblioref_referencesource` AS c ON b.`source_id` = c.`reference_ptr_id`
WHERE LEFT(a.`literature_type`,1) <> 'S'
  AND LEFT(a.`treatment_level`,1) = 'a'
  AND b.`english_translated_title` NOT IN ('', 'x');
