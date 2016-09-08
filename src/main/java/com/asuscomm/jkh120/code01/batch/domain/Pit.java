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
public class Pit {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;
	
	private int ab;
	private int bbhp;
	private String btop;
	private String cpPersonId;
	private int er;
	private double era;
	private Long gameId;
	private int gp;
	private int hit;
	private int hr;
	private String ip;
	private int l;
	private int np;
	private int pa;
	private String personFirstKname;
	private String personFullKname;
	private Long personId;
	private String personLastKname;
	private int r;
	private int so;
	private int sv;
	private String teamFullName;
	private Long teamId;
	private String teamName;
	private int w;
	private double whip;
	private String wls;
	
	public Pit() {
		super();
		// TODO Auto-generated constructor stub
	}
	
}
