-- BBC Proteomes Database Schema (MySQL)
-- Charset
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;

-- Core proteome table (flat identity + file stats)
CREATE TABLE IF NOT EXISTS proteome (
  hash VARCHAR(128) PRIMARY KEY,
  origin_cp VARCHAR(255),
  web VARCHAR(1024),
  in_ncbi_refseq TINYINT(1) DEFAULT 0,
  species VARCHAR(255),
  taxid BIGINT,
  species_taxid BIGINT,
  current_scientific_name VARCHAR(255),
  common_names VARCHAR(1024),
  group_name VARCHAR(255),
  informal_clade VARCHAR(255),
  code_vfv VARCHAR(128),
  file_name VARCHAR(255),
  filepath_original VARCHAR(1024),
  num_seqs INT,
  sum_len BIGINT,
  min_len INT,
  avg_len DOUBLE,
  max_len INT,
  file_snip_name VARCHAR(255),
  filepath_snip_processed VARCHAR(1024),
  filepath_renamed_vfv VARCHAR(1024),
  num_seqs_snip_processed INT,
  sum_len_snip_processed BIGINT,
  min_len_snip_processed INT,
  avg_len_snip_processed DOUBLE,
  max_len_snip_processed INT,
  post_snip TINYINT(1) DEFAULT 0,
  assembly_id VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_species (species),
  INDEX idx_taxid (taxid),
  INDEX idx_code_vfv (code_vfv)
);

-- BUSCO summary per rank (Domain/Kingdom)
CREATE TABLE IF NOT EXISTS busco_summary (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  hash VARCHAR(128) NOT NULL,
  `rank` ENUM('Domain','Kingdom') NOT NULL,
  complete DOUBLE,
  single_copy DOUBLE,
  duplicated DOUBLE,
  fragmented DOUBLE,
  missing DOUBLE,
  FOREIGN KEY (hash) REFERENCES proteome(hash) ON DELETE CASCADE,
  UNIQUE KEY uniq_hash_rank (hash, `rank`)
);

-- Taxonomy terms (deduplicated across proteomes)
CREATE TABLE IF NOT EXISTS taxonomy_term (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  level ENUM(
    'Domain','Realm','Kingdom','Subkingdom','Superphylum','Phylum','Subphylum','Infraphylum',
    'Superclass','Class','Subclass','Infraclass','Cohort','Subcohort','Superorder','Order',
    'Suborder','Infraorder','Parvorder','Superfamily','Family','Subfamily','Tribe','Subtribe',
    'Genus','Subgenus','Section','Subsection','Series','Subseries','Species_group','Species_subgroup',
    'Forma_specialis','Subspecies','VarietasSubvariety','Forma','Serogroup','Serotype','Strain','Isolate',
    'Species'
  ) NOT NULL,
  name VARCHAR(255) NOT NULL,
  UNIQUE KEY uniq_level_name (level, name),
  INDEX idx_level (level),
  INDEX idx_name (name)
);

-- Mapping of proteomes to taxonomy terms
CREATE TABLE IF NOT EXISTS proteome_taxonomy (
  hash VARCHAR(128) NOT NULL,
  term_id BIGINT NOT NULL,
  PRIMARY KEY (hash, term_id),
  FOREIGN KEY (hash) REFERENCES proteome(hash) ON DELETE CASCADE,
  FOREIGN KEY (term_id) REFERENCES taxonomy_term(id) ON DELETE CASCADE,
  INDEX idx_term (term_id)
);

-- Collections defined by name
CREATE TABLE IF NOT EXISTS collection (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  UNIQUE KEY uniq_collection_name (name)
);

-- Membership of proteomes in collections
CREATE TABLE IF NOT EXISTS collection_membership (
  collection_id BIGINT NOT NULL,
  hash VARCHAR(128) NOT NULL,
  PRIMARY KEY (collection_id, hash),
  FOREIGN KEY (collection_id) REFERENCES collection(id) ON DELETE CASCADE,
  FOREIGN KEY (hash) REFERENCES proteome(hash) ON DELETE CASCADE,
  INDEX idx_member_hash (hash)
);

-- Create a new view name to avoid needing privileges on the old definer-owned view
CREATE VIEW view_proteomes_flat_v2 AS
SELECT
  p.hash,
  p.origin_cp AS `Origin CP`,
  p.web AS `web`,
  p.in_ncbi_refseq AS `In NCBI's ref seq`,
  p.species AS `Species`,
  p.taxid AS `taxID`,
  p.species_taxid AS `species_taxid`,
  p.current_scientific_name AS `current_scientific_name`,
  p.common_names AS `common_names`,
  p.group_name AS `group_name`,
  p.informal_clade AS `informal_clade`,
  p.code_vfv AS `code_vFV`,
  p.file_name AS `File_name`,
  p.filepath_original AS `Filepath_original`,
  p.num_seqs AS `num_seqs`,
  p.sum_len AS `sum_len`,
  p.min_len AS `min_len`,
  p.avg_len AS `avg_len`,
  p.max_len AS `max_len`,
  p.file_snip_name AS `File_snip_name`,
  p.filepath_snip_processed AS `Filepath_snip_processed`,
  p.filepath_renamed_vfv AS `Filepath_renamed_vFV`,
  p.num_seqs_snip_processed AS `num_seqs_snip_processed`,
  p.sum_len_snip_processed AS `sum_len_snip_processed`,
  p.min_len_snip_processed AS `min_len_snip_processed`,
  p.avg_len_snip_processed AS `avg_len_snip_processed`,
  p.max_len_snip_processed AS `max_len_snip_processed`,
  p.post_snip AS `post_snip`,
  p.assembly_id AS `AssemblyID`,
  -- BUSCO Domain
  (SELECT bs.complete FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Domain') AS `Complete BUSCO Domain`,
  (SELECT bs.single_copy FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Domain') AS `Single BUSCO Domain`,
  (SELECT bs.duplicated FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Domain') AS `Duplicated BUSCO Domain`,
  (SELECT bs.fragmented FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Domain') AS `Fragmented BUSCO Domain`,
  (SELECT bs.missing FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Domain') AS `Missing BUSCO Domain`,
  -- BUSCO Kingdom
  (SELECT bs.complete FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Kingdom') AS `Complete BUSCO Kingdom`,
  (SELECT bs.single_copy FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Kingdom') AS `Single BUSCO Kingdom`,
  (SELECT bs.duplicated FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Kingdom') AS `Duplicated BUSCO Kingdom`,
  (SELECT bs.fragmented FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Kingdom') AS `Fragmented BUSCO Kingdom`,
  (SELECT bs.missing FROM busco_summary bs WHERE bs.hash = p.hash AND bs.`rank`='Kingdom') AS `Missing BUSCO Kingdom`,
  -- Taxonomy (single name per level if present)
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Domain' LIMIT 1) AS `Domain`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Realm' LIMIT 1) AS `Realm`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Kingdom' LIMIT 1) AS `Kingdom`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subkingdom' LIMIT 1) AS `Subkingdom`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Superphylum' LIMIT 1) AS `Superphylum`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Phylum' LIMIT 1) AS `Phylum`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subphylum' LIMIT 1) AS `Subphylum`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Infraphylum' LIMIT 1) AS `Infraphylum`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Superclass' LIMIT 1) AS `Superclass`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Class' LIMIT 1) AS `Class`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subclass' LIMIT 1) AS `Subclass`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Infraclass' LIMIT 1) AS `Infraclass`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Cohort' LIMIT 1) AS `Cohort`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subcohort' LIMIT 1) AS `Subcohort`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Superorder' LIMIT 1) AS `Superorder`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Order' LIMIT 1) AS `Order`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Suborder' LIMIT 1) AS `Suborder`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Infraorder' LIMIT 1) AS `Infraorder`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Parvorder' LIMIT 1) AS `Parvorder`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Superfamily' LIMIT 1) AS `Superfamily`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Family' LIMIT 1) AS `Family`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subfamily' LIMIT 1) AS `Subfamily`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Tribe' LIMIT 1) AS `Tribe`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subtribe' LIMIT 1) AS `Subtribe`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Genus' LIMIT 1) AS `Genus`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subgenus' LIMIT 1) AS `Subgenus`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Section' LIMIT 1) AS `Section`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subsection' LIMIT 1) AS `Subsection`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Series' LIMIT 1) AS `Series`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subseries' LIMIT 1) AS `Subseries`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Species_group' LIMIT 1) AS `Species_group`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Species_subgroup' LIMIT 1) AS `Species_subgroup`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Forma_specialis' LIMIT 1) AS `Forma_specialis`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Subspecies' LIMIT 1) AS `Subspecies`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='VarietasSubvariety' LIMIT 1) AS `VarietasSubvariety`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Forma' LIMIT 1) AS `Forma`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Serogroup' LIMIT 1) AS `Serogroup`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Serotype' LIMIT 1) AS `Serotype`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Strain' LIMIT 1) AS `Strain`,
  (SELECT tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=p.hash AND tt.level='Isolate' LIMIT 1) AS `Isolate`
FROM proteome p;

SET FOREIGN_KEY_CHECKS=1;
