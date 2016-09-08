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
@ToString(exclude="id")
@Entity
public class Bat {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	private int ab;
	private double avg;
	private String btop;
	private Long gameId;
	private int hit;
	private String inn1;
	private String inn2;
	private String inn3;
	private String inn4;
	private String inn5;
	private String inn6;
	private String inn7;
	private String inn8;
	private String inn9;
	private String inn10;
	private String inn11;
	private String inn12;
	private String inn13;
	private String inn14;
	private String inn15;
	private String personFirstKname;
	private String personFullKname;
	private Long personId;
	private String personLastKname;
	private String position;
	private int r;
	private int rbi;
	private String teamFullName;
	private Long teamId;
	private String teamName;
	
	public Bat() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
}
