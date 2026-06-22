SELECT 'LILACS_Sas' AS dbase,
  b.`reference_ptr_id` AS `id`,
  b.`english_translated_title` AS `title`,
  c.`title_serial`,
  SUBSTRING(a.`publication_date_normalized`, 1, 4) AS publication_year,
  c.`volume_serial`,
  c.`issue_number`,
  IFNULL(b.`individual_author`, b.`corporate_author`) AS author,
  b.`pages`,
  a.`cooperative_center_code`,
  a.`literature_type`,
  a.`treatment_level`,
  a.`status`,
  CASE 
	WHEN a.`electronic_address` IS NULL THEN ''
	WHEN a.`electronic_address` = '[]' THEN ''
	ELSE a.`electronic_address`
  END AS electronic_address,
  CONCAT('https://fi-admin.bvsalud.org/bibliographic/edit-analytic/',
  b.`reference_ptr_id`) AS link_fiadmin
FROM `biblioref_referenceanalytic` AS b
JOIN `biblioref_referencesource` AS c
ON b.`source_id` = c.`reference_ptr_id`
JOIN `biblioref_reference` AS a
ON b.`source_id` = a.`id`
WHERE b.`english_translated_title` <> ''
AND b.`english_translated_title` <> 'x'
AND LEFT(a.`literature_type`, 1) = 'S';
