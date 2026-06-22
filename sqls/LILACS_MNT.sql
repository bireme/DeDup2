SELECT 'LILACS_MNT' AS dbase,
      a.`id`,
      b.`title_monographic`,
      SUBSTRING(a.`publication_date_normalized`,1,4) AS publication_year,
      IFNULL(b.`individual_author_monographic`,b.`corporate_author_monographic`) AS author,
      b.`pages_monographic`,
      b.`volume_serial`,
      b.`issue_number`
FROM
     `biblioref_reference` AS a
JOIN `biblioref_referencesource` AS b
ON a.`id` = b.`reference_ptr_id`
WHERE LEFT(a.`literature_type`, 1) IN ('M','N','T');
