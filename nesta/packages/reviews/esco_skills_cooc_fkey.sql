ALTER TABLE esco_skill_cooccurrences
  ADD CONSTRAINT FK_skill_1 FOREIGN KEY (skill_1)
    REFERENCES esco_skills.id
  ADD CONSTRAINT FK_skill_2 FOREIGN KEY (skill_2)
    REFERENCES esco_skills.id
  ON DELETE CASCADE
  ON UPDATE CASCADE
;
