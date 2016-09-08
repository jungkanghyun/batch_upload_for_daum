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
public class Report {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	private Long gameId;
	private String cpGameId;
	private String btop;
	private Long teamId;
	private String cpTeamId;
	private String teamName;
	private String teamFullName;
	private int hit;
	private int hr;
	private int sb;
	private int err;
	private int gdp;
	private int so;
	private double avg;
	private int lob;
	private int bbhp;
	
	public Report() {
		super();
		// TODO Auto-generated constructor stub
	}
}
