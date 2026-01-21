-- phpMyAdmin SQL Dump
-- version 5.2.3
-- https://www.phpmyadmin.net/
--
-- Host: mysql
-- Generation Time: Dec 11, 2025 at 04:08 PM
-- Server version: 9.5.0
-- PHP Version: 8.3.27

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `atp2`
--

-- --------------------------------------------------------

--
-- Table structure for table `countries`
--

CREATE TABLE `countries` (
  `country_id` int NOT NULL,
  `country_name` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `match_info`
--

CREATE TABLE `match_info` (
  `game_id` int NOT NULL,
  `player_name` varchar(100) NOT NULL,
  `ground` varchar(10) DEFAULT NULL,
  `oponent` varchar(255) DEFAULT NULL,
  `wl` varchar(10) DEFAULT NULL,
  `match_id` varchar(255) NOT NULL,
  `tournament_id` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `player_info`
--

CREATE TABLE `player_info` (
  `player_id` int NOT NULL,
  `hand` varchar(10) DEFAULT NULL,
  `born_country` varchar(255) DEFAULT NULL,
  `player_name` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `tournaments`
--

CREATE TABLE `tournaments` (
  `tournament_id` int NOT NULL,
  `location` varchar(255) DEFAULT NULL,
  `tournament_name` varchar(255) NOT NULL,
  `start_date` date NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `countries`
--
ALTER TABLE `countries`
  ADD PRIMARY KEY (`country_id`),
  ADD UNIQUE KEY `country_name` (`country_name`);

--
-- Indexes for table `match_info`
--
ALTER TABLE `match_info`
  ADD PRIMARY KEY (`game_id`),
  ADD KEY `player_name` (`player_name`),
  ADD KEY `fk_match_tournament` (`tournament_id`);

--
-- Indexes for table `player_info`
--
ALTER TABLE `player_info`
  ADD PRIMARY KEY (`player_id`),
  ADD UNIQUE KEY `player_name` (`player_name`),
  ADD KEY `born_country` (`born_country`);

--
-- Indexes for table `tournaments`
--
ALTER TABLE `tournaments`
  ADD PRIMARY KEY (`tournament_id`),
  ADD KEY `location` (`location`),
  ADD KEY `idx_tournament_name` (`tournament_name`),
  ADD KEY `idx_start_date` (`start_date`),
  ADD KEY `idx_tournament_name_date` (`tournament_name`,`start_date`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `countries`
--
ALTER TABLE `countries`
  MODIFY `country_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `match_info`
--
ALTER TABLE `match_info`
  MODIFY `game_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `player_info`
--
ALTER TABLE `player_info`
  MODIFY `player_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `tournaments`
--
ALTER TABLE `tournaments`
  MODIFY `tournament_id` int NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `match_info`
--
ALTER TABLE `match_info`
  ADD CONSTRAINT `fk_match_tournament` FOREIGN KEY (`tournament_id`) REFERENCES `tournaments` (`tournament_id`) ON DELETE RESTRICT ON UPDATE CASCADE,
  ADD CONSTRAINT `match_info_ibfk_1` FOREIGN KEY (`player_name`) REFERENCES `player_info` (`player_name`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `player_info`
--
ALTER TABLE `player_info`
  ADD CONSTRAINT `player_info_ibfk_1` FOREIGN KEY (`born_country`) REFERENCES `countries` (`country_name`) ON DELETE SET NULL ON UPDATE CASCADE;

--
-- Constraints for table `tournaments`
--
ALTER TABLE `tournaments`
  ADD CONSTRAINT `tournaments_ibfk_1` FOREIGN KEY (`location`) REFERENCES `countries` (`country_name`) ON DELETE SET NULL ON UPDATE CASCADE;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
