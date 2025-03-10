ENABLE SERVER 'd513c7d2-e428-46c0-902e-cd33a041eddc';
SHOW DATABASES
CREATE DATABASE foo TOPOLOGY 1 PRIMARIES 2 SECONDARIES
ALTER DATABASE neo4j SET TOPOLOGY 1 PRIMARIES 2 SECONDARY



CREATE INDEX fragment_content_hash_index FOR (n:Fragment) ON (n.content_hash);
CREATE INDEX tag_name_index FOR (n:Tag) ON (n.name);
CREATE INDEX thread_full_text_index FOR (n:Thread) ON (n.uuid, n.title, n.summary);
CREATE INDEX thread_uuid_index FOR (n:Thread) ON (n.uuid);
CREATE FULLTEXT INDEX threadsAndFragmentsIndex FOR (n:Thread|Fragment) ON EACH [n.title, n.summary, n.content];
CREATE FULLTEXT INDEX userSearchIndex FOR (n:User) ON EACH [n.name];
CREATE INDEX user_unique_index FOR (n:User) ON (n.uuid, n.name, n.email);
CREATE INDEX thread_tag_relationship_index FOR ()-[r:HAS_TAG]->() ON r.uuid

