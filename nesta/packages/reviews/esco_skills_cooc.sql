INSERT INTO esco_skill_cooccurrences (skill_1, skill_2, cooccurrences)
SELECT
  t1.skill_id AS skill_1,
  t2.skill_id AS skill_2,
  COUNT(*) AS cooccurrences
FROM esco_occupation_skills AS t1
  JOIN esco_occupation_skills AS t2
  ON t1.occupation_id = t2.occupation_id
WHERE t1.skill_id < t2.skill_id
GROUP BY skill_1, skill_2
;
