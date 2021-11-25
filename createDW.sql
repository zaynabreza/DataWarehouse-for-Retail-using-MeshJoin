CREATE DATABASE  IF NOT EXISTS `dwh`;
USE `dwh`;

DROP TABLE IF EXISTS `transaction_fact`;

--
-- Table structure for table `customer`
--

DROP TABLE IF EXISTS `customer`;
CREATE TABLE `customer` (
  `customer_id` varchar(4) NOT NULL,
  `customer_name` varchar(30) NOT NULL,
  PRIMARY KEY (`customer_id`)
);

--
-- Table structure for table `date`
--

DROP TABLE IF EXISTS `date`;
CREATE TABLE `date` (
  `date` date NOT NULL,
  `day` varchar(10) NOT NULL,
  `month` int NOT NULL,
  `quarter` int NOT NULL,
  `year` int NOT NULL,
  PRIMARY KEY (`date`)
);

--
-- Table structure for table `product`
--

DROP TABLE IF EXISTS `product`;
CREATE TABLE `product` (
  `product_id` varchar(6) NOT NULL,
  `product_name` varchar(30) NOT NULL,
  PRIMARY KEY (`product_id`)
);

--
-- Table structure for table `store`
--

DROP TABLE IF EXISTS `store`;
CREATE TABLE `store` (
  `store_id` varchar(4) NOT NULL,
  `store_name` varchar(20) NOT NULL,
  PRIMARY KEY (`store_id`)
);

--
-- Table structure for table `supplier`
--

DROP TABLE IF EXISTS `supplier`;
CREATE TABLE `supplier` (
  `supplier_id` varchar(5) NOT NULL,
  `supplier_name` varchar(30) NOT NULL,
  PRIMARY KEY (`supplier_id`)
);
--
-- Table structure for table `transaction_fact`
--


CREATE TABLE `transaction_fact` (
  `transaction_id` int NOT NULL,
  `product_id` varchar(6) NOT NULL,
  `customer_id` varchar(4) NOT NULL,
  `store_id` varchar(4) NOT NULL,
  `date_id` date NOT NULL,
  `supplier_id` varchar(5) NOT NULL,
  `quantity` smallint NOT NULL,
  `total_sale` decimal(7,2) NOT NULL,
  PRIMARY KEY (`transaction_id`),
  KEY `customer_idx` (`customer_id`),
  KEY `store_idx` (`store_id`),
  KEY `date_idx` (`date_id`),
  KEY `product` (`product_id`),
  KEY `supplier_idx` (`supplier_id`),
  CONSTRAINT `customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`),
  CONSTRAINT `date` FOREIGN KEY (`date_id`) REFERENCES `date` (`date`),
  CONSTRAINT `product` FOREIGN KEY (`product_id`) REFERENCES `product` (`product_id`),
  CONSTRAINT `store` FOREIGN KEY (`store_id`) REFERENCES `store` (`store_id`),
  CONSTRAINT `supplier` FOREIGN KEY (`supplier_id`) REFERENCES `supplier` (`supplier_id`)
);

