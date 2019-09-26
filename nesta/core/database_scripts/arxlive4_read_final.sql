SELECT
    a.id AS article_id,
    a.created AS article_created,
    a.created AS article_updated,    
    a.citation_count,
    b.arxiv_category_ids,
    b.arxiv_category_descs,
    b.institute_country,
    b.institute_city,
    b.is_multinational,
    b.institute_name
FROM arxiv_articles a 
JOIN tmp_article_cats b ON a.id = b.article_id;
