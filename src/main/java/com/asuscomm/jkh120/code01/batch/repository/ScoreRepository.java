package com.asuscomm.jkh120.code01.batch.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.asuscomm.jkh120.code01.batch.domain.Score;

@Repository
public interface ScoreRepository extends JpaRepository<Score, Long> {

	public List<Score> findByGameId(Long gameId);
}
