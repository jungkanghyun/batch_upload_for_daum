package com.asuscomm.jkh120.code01.batch.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.asuscomm.jkh120.code01.batch.domain.Pit;

@Repository
public interface PitRepository extends JpaRepository<Pit, Long> {

	public List<Pit> findByGameId(Long gameId);
}
