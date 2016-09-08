package com.asuscomm.jkh120.code01.batch.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.asuscomm.jkh120.code01.batch.domain.Game;

@Repository
public interface GameRepository extends JpaRepository<Game, Long> {

}
