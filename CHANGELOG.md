# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- The broker no longer writes a Produce response for an `acks=0` request. A storage or validation failure on an `acks=0` Produce now closes the connection (matching Apache Kafka) instead of silently dropping the batch.
