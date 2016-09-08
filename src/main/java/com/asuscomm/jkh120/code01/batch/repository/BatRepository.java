package com.asuscomm.jkh120.code01.batch.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.asuscomm.jkh120.code01.batch.domain.Bat;



@Repository
public interface BatRepository extends JpaRepository <Bat, Long> {

	public List<Bat> findByGameId(Long gameId);
	
}
