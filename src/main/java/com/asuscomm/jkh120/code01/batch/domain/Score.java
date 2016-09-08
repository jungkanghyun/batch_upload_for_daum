package com.asuscomm.jkh120.code01.batch.domain;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Entity
@ToString(exclude="id")
public class Score {
	
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;
	
	private int ballFour;
	private String btop;
	private String cpGameId;
	private String cpTeamId;
	private int error;
	private String gameDay;
	private Long gameId;
	private String gameMonth;
	private int hit;
	private int inn1;
	private int inn2;
	private int inn3;
	private int inn4;
	private int inn5;
	private int inn6;
	private int inn7;
	private int inn8;
	private int inn9;
	private int inn10;
	private int inn11;
	private int inn12;
	private int inn13;
	private int inn14;
	private int inn15;
	private int inn16;
	private int inn17;
	private int inn18;
	private int inn19;
	private int inn20;
	private int run;
	private String seasonId;
	private String teamFullName;
	private Long teamId;
	private String teamName;

	public Score() {
		super();
		// TODO Auto-generated constructor stub
	}
}
