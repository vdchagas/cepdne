CREATE TABLE IF NOT EXISTS postcode_correios (
  id_postcode_correios int NOT NULL AUTO_INCREMENT,
  cep varchar(16) NOT NULL,
  street varchar(200) DEFAULT NULL,
  city varchar(200) DEFAULT NULL,
  region varchar(4) DEFAULT NULL,
  neighborhood varchar(144) DEFAULT NULL,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id_postcode_correios),
  UNIQUE KEY cep(cep),
  KEY ix_postcode_correios_city (city),
  KEY ix_cep (cep)
) ENGINE=InnoDB;
