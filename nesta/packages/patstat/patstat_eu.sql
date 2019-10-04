SET group_concat_max_len=16384;


-- Granted app family, > 2000, EU people
CREATE TEMPORARY TABLE tmp_families
       (PRIMARY KEY (docdb_family_id))
SELECT DISTINCT(docdb_family_id)
FROM tls207_pers_appln
INNER JOIN
      tls201_appln on tls207_pers_appln.appln_id = tls201_appln.appln_id
INNER JOIN
      tls906_person on tls207_pers_appln.person_id = tls906_person.person_id
WHERE
      tls201_appln.earliest_publn_year > 2000
      AND
      tls201_appln.granted = 'Y'
      AND
      tls201_appln.docdb_family_size > 1
      AND
      tls906_person.person_ctry_code IN (SELECT ctry_code
					 FROM tls801_country
					 WHERE eu_member = 'Y')
LIMIT :limit;


-- Granted apps w/ family, > 2000, EU people
CREATE TEMPORARY TABLE tmp_appln_families
       (PRIMARY	KEY (docdb_family_id, appln_id))
SELECT appln_id, 
       a.docdb_family_id AS docdb_family_id,
       appln_filing_date,
       appln_filing_year,
       appln_auth,
       appln_nr,
       appln_kind,
       ipr_type,
       receiving_office,
       granted,
       nb_citing_docdb_fam       
FROM tls201_appln a
LEFT JOIN tmp_families b ON a.docdb_family_id = b.docdb_family_id
WHERE b.docdb_family_id IS NOT NULL
AND a.granted = 'Y';


-- Output 1: Granted apps grouped by family, > 2000, EU people
CREATE TEMPORARY TABLE tmp_appln_fam_groups
SELECT docdb_family_id,
       MAX(nb_citing_docdb_fam) as nb_citing_docdb_fam,
       MIN(appln_filing_date) as earliest_filing_date,
       MIN(appln_filing_year) as earliest_filing_year,       
       GROUP_CONCAT(appln_id ORDER BY appln_filing_date) as appln_id,
       GROUP_CONCAT(DISTINCT appln_auth ORDER BY appln_filing_date) as appln_auth
FROM tmp_appln_families
GROUP BY docdb_family_id;


-- Granted apps w/out family, > 2000, EU people
CREATE TEMPORARY TABLE tmp_applnid_no_fam
SELECT DISTINCT(tls201_appln.appln_id)
FROM tls207_pers_appln
INNER JOIN
      tls201_appln ON tls207_pers_appln.appln_id = tls201_appln.appln_id
INNER JOIN
      tls906_person ON tls207_pers_appln.person_id = tls906_person.person_id
WHERE
      tls201_appln.earliest_publn_year > 2000
      AND
      tls201_appln.granted = 'Y'
      AND
      tls201_appln.docdb_family_size = 1
      AND
      tls906_person.person_ctry_code IN (SELECT ctry_code
                                         FROM tls801_country
                                         WHERE eu_member = 'Y')
LIMIT :limit;


-- Output 2: Granted apps w/out family, > 2000, EU people, w/ abstract
CREATE TEMPORARY TABLE tmp_appln_no_fam
SELECT a.appln_id AS appln_id,
       appln_auth,
       nb_citing_docdb_fam,
       earliest_filing_date,
       earliest_filing_year,
       docdb_family_id
FROM tmp_applnid_no_fam a
LEFT JOIN tls201_appln b ON a.appln_id = b.appln_id
LEFT JOIN tls203_appln_abstr c ON a.appln_id = c.appln_id
WHERE c.appln_id IS NOT NULL;
