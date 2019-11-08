CREATE TEMPORARY TABLE tmp_article_ids_2
SELECT 
    a.article_id as article_id,
    CONCAT(gi.city, ", ", gi.country_code) as city,
    gi.country as country,
    ai.is_multinational as is_multinational,
    gi.name as name
FROM tmp_article_ids a
JOIN arxiv_article_institutes ai ON ai.article_id = a.article_id
JOIN grid_institutes gi ON gi.id = ai.institute_id
WHERE ai.matching_score >= 0.9;
