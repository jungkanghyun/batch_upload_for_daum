package com.asuscomm.jkh120.code01.batch.configuration;

import java.util.List;
import java.util.Map;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.asuscomm.jkh120.code01.batch.domain.Bat;
import com.asuscomm.jkh120.code01.batch.domain.Game;
import com.asuscomm.jkh120.code01.batch.domain.Pit;
import com.asuscomm.jkh120.code01.batch.domain.Report;
import com.asuscomm.jkh120.code01.batch.domain.Score;
import com.asuscomm.jkh120.code01.batch.repository.BatRepository;
import com.asuscomm.jkh120.code01.batch.repository.GameRepository;
import com.asuscomm.jkh120.code01.batch.repository.PitRepository;
import com.asuscomm.jkh120.code01.batch.repository.ReportRepository;
import com.asuscomm.jkh120.code01.batch.repository.ScoreRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class StepConfiguration {
	private static final String STEP_NAME = "dbLoaderStep";

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MultiResourceItemReader multiReader;

	@Bean
	public Step step() {
		return stepBuilderFactory.get(STEP_NAME).<Map<String, Object>, Map<String, Object>>chunk(1)
				.reader(reportReader())
				// .processor(itemProcessor())
				.writer(reportWriter())
				// .reader(reportReader())
				.build();
	}

	@Bean
	public ItemReader<Map<String, Object>> reportReader() {
		return multiReader;
	}

	@Bean
	public MultiResourceItemReader multiReader() {
		ReportLineMapper lineMapper = new ReportLineMapper();

		FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader<>();
		reader.setLineMapper(lineMapper);
		MultiResourceItemReader<Map<String, Object>> multiReader = new MultiResourceItemReader<>();
		multiReader.setDelegate(reader);

		return multiReader;
	}

	// @Bean
	// public ItemProcessor<Map<String, Object>, Map<String, Object>>
	// itemProcessor() {
	// SpringValidator<ArrayList<Report>> springValidator = new
	// SpringValidator<>();
	//// springValidator.setValidator(validator);
	// ValidatingItemProcessor<ArrayList<Report>> ValidatingItemProcessor = new
	// ValidatingItemProcessor<>();
	// ValidatingItemProcessor.setValidator(springValidator);
	//
	// return ValidatingItemProcessor;
	// }

	@Bean
	public ItemWriter<Map<String, Object>> reportWriter() {
		ReportItemWriter writer = new ReportItemWriter();
		return writer;
	}

	class ReportLineMapper implements LineMapper<Map<String, Object>> {

		@Override
		public Map<String, Object> mapLine(String line, int lineNumber) throws Exception {

			ObjectMapper objectMapper = new ObjectMapper();
			Map<String, Object> reportMap;
			reportMap = objectMapper.readValue(line, Map.class);

			return reportMap;
		}

	}

	class ReportItemWriter implements ItemWriter<Map<String, Object>> {

		@Autowired
		private ReportRepository reportRepository;

		@Autowired
		private ScoreRepository scoreRepository;

		@Autowired
		private PitRepository pitRepository;

		@Autowired
		private BatRepository batRepository;
		
		@Autowired
		private GameRepository gameRepository;

		@Override
		public void write(List<? extends Map<String, Object>> items) throws Exception {

			for (Map<String, Object> entity : items) {

				
				Map<String, Object> paramsMap = (Map<String, Object>) entity.get("params");
				Long gameId = Long.parseLong((String) paramsMap.get("game_id"));
				
				System.out.println(gameId);
				
				List<Object> graphList = (List<Object>) entity.get("graph");
				
				if (!graphList.isEmpty()) {
					List<Report> graphsList = reportRepository.findByGameId(gameId);
					reportRepository.delete(graphsList);					
				}


				for (Object graph : graphList) {

					Map<String, Object> graphMap = (Map<String, Object>) graph;

					Report report = new Report();
					// map fields
					report.setAvg((Double) graphMap.get("AVG"));
					report.setBbhp((int) graphMap.get("BBHP"));
					report.setBtop((String) graphMap.get("BTOP"));
					report.setCpGameId((String) graphMap.get("CP_GAME_ID"));
					report.setCpTeamId((String) graphMap.get("CP_TEAM_ID"));
					report.setErr((int) graphMap.get("ERR"));
					report.setGameId(Integer.toUnsignedLong((int) graphMap.get("GAME_ID")));
					report.setGdp((int) graphMap.get("GDP"));
					report.setHit((int) graphMap.get("HIT"));
					report.setHr((int) graphMap.get("HR"));
					// report.setId((Long) graphMap.get("ID"));
					report.setLob((int) graphMap.get("LOB"));
					report.setSb((int) graphMap.get("SB"));
					report.setSo((int) graphMap.get("SO"));
					report.setTeamFullName((String) graphMap.get("TEAM_FULL_NAME"));
					report.setTeamId(Integer.toUnsignedLong((int) graphMap.get("TEAM_ID")));
					report.setTeamName((String) graphMap.get("TEAM_NAME"));

					reportRepository.save(report);
				}
				reportRepository.flush();

				List<Object> scoreList = (List<Object>) entity.get("score");
				
				if (!scoreList.isEmpty()) {
					List<Score> scoresList = scoreRepository.findByGameId(gameId);
					scoreRepository.delete(scoresList);					
				}


				for (Object scores : scoreList) {

					Map<String, Object> scoreMap = (Map<String, Object>) scores;

					Score score = new Score();
					// map fields
					score.setBallFour((int) scoreMap.getOrDefault("BALLFOUR", 0));
					score.setBtop((String) scoreMap.get("BTOP"));
					score.setCpGameId((String) scoreMap.get("CP_GAME_ID"));
					score.setCpTeamId((String) scoreMap.get("CP_TEAM_ID"));
					score.setError((int) scoreMap.get("ERROR"));
					score.setGameId(Integer.toUnsignedLong((int) scoreMap.get("GAME_ID")));
					score.setGameDay((String) scoreMap.get("GAME_DAY"));
					score.setGameMonth((String) scoreMap.get("GAME_MONTH"));
					score.setError((int) scoreMap.get("ERROR"));
					score.setHit((int) scoreMap.get("HIT"));
					score.setRun((int) scoreMap.get("RUN"));
					score.setInn1((int) scoreMap.get("INN1"));
					score.setInn2((int) scoreMap.get("INN2"));
					score.setInn3((int) scoreMap.get("INN3"));
					score.setInn4((int) scoreMap.get("INN4"));
					score.setInn5((int) scoreMap.get("INN5"));
					score.setInn6((int) scoreMap.get("INN6"));
					score.setInn7((int) scoreMap.get("INN7"));
					score.setInn8((int) scoreMap.get("INN8"));
					score.setInn9((int) scoreMap.get("INN9"));
					score.setInn10((int) scoreMap.get("INN10"));
					score.setInn11((int) scoreMap.get("INN11"));
					score.setInn12((int) scoreMap.get("INN12"));
					score.setInn13((int) scoreMap.get("INN13"));
					score.setInn14((int) scoreMap.get("INN14"));
					score.setInn15((int) scoreMap.get("INN15"));
					score.setInn16((int) scoreMap.get("INN16"));
					score.setInn17((int) scoreMap.get("INN17"));
					score.setInn18((int) scoreMap.get("INN18"));
					score.setInn19((int) scoreMap.get("INN19"));
					score.setInn20((int) scoreMap.get("INN20"));
					score.setSeasonId((String) scoreMap.get("SEASON_ID"));
					score.setTeamFullName((String) scoreMap.get("TEAM_FULL_NAME"));
					score.setTeamId(Integer.toUnsignedLong((int) scoreMap.get("TEAM_ID")));
					score.setTeamName((String) scoreMap.get("TEAM_NAME"));

					scoreRepository.save(score);
				}
				scoreRepository.flush();

				List<Object> batList = (List<Object>) entity.get("bat");

				if (!batList.isEmpty()) {
					List<Bat> batsList = batRepository.findByGameId(gameId);
					batRepository.delete(batsList);					
				}
				
				for (Object bats : batList) {

					Map<String, Object> batMap = (Map<String, Object>) bats;

					Bat bat = new Bat();
					// map fields
					bat.setAb((int) batMap.get("AB"));
					bat.setAvg((Double) batMap.get("AVG"));
					bat.setBtop((String) batMap.get("BTOP"));
					bat.setGameId(Integer.toUnsignedLong((int) batMap.get("GAME_ID")));
					bat.setHit((int) batMap.get("HIT"));
					bat.setR((int) batMap.get("R"));
					bat.setRbi((int) batMap.get("RBI"));
					bat.setPersonFirstKname((String) batMap.get("PERSON_FIRST_KNAME"));
					bat.setPersonFullKname((String) batMap.get("PERSON_FULL_KNAME"));
					bat.setPersonLastKname((String) batMap.get("PERSON_LAST_KNAME"));
					bat.setPersonId(Integer.toUnsignedLong((int) batMap.get("PERSON_ID")));
					bat.setInn1((String) batMap.get("INN1"));
					bat.setInn2((String) batMap.get("INN2"));
					bat.setInn3((String) batMap.get("INN3"));
					bat.setInn4((String) batMap.get("INN4"));
					bat.setInn5((String) batMap.get("INN5"));
					bat.setInn6((String) batMap.get("INN6"));
					bat.setInn7((String) batMap.get("INN7"));
					bat.setInn8((String) batMap.get("INN8"));
					bat.setInn9((String) batMap.get("INN9"));
					bat.setInn10((String) batMap.get("INN10"));
					bat.setInn11((String) batMap.get("INN11"));
					bat.setInn12((String) batMap.get("INN12"));
					bat.setInn13((String) batMap.get("INN13"));
					bat.setInn14((String) batMap.get("INN14"));
					bat.setInn15((String) batMap.get("INN15"));
					bat.setPosition((String) batMap.get("POSITION"));
					bat.setTeamFullName((String) batMap.get("TEAM_FULL_NAME"));
					bat.setTeamId(Integer.toUnsignedLong((int) batMap.get("TEAM_ID")));
					bat.setTeamName((String) batMap.get("TEAM_NAME"));

					batRepository.save(bat);
				}
				batRepository.flush();
				
				List<Object> pitList = (List<Object>) entity.get("pit");
				
				if (!pitList.isEmpty()) {
					List<Pit> pitsList = pitRepository.findByGameId(gameId);
					pitRepository.delete(pitsList);					
				}

				for (Object pits : pitList) {

					Map<String, Object> pitMap = (Map<String, Object>) pits;

					Pit pit = new Pit();
					// map fields
					pit.setAb((int) pitMap.get("AB"));
					pit.setBbhp((int) pitMap.get("BBHP"));
					pit.setBtop((String) pitMap.get("BTOP"));
					pit.setCpPersonId((String) pitMap.get("CP_PERSON_ID"));
					pit.setEr((int) pitMap.get("ER"));
					pit.setEra((Double) pitMap.get("ERA"));
					pit.setGameId(Integer.toUnsignedLong((int) pitMap.get("GAME_ID")));
					pit.setGp((int) pitMap.get("GP"));
					pit.setHit((int) pitMap.get("HIT"));
					pit.setHr((int) pitMap.get("HR"));
					pit.setIp((String) pitMap.get("IP"));
					pit.setL((int) pitMap.get("L"));
					pit.setNp((int) pitMap.get("NP"));
					pit.setPa((int) pitMap.get("PA"));
					pit.setR((int) pitMap.get("R"));
					pit.setSo((int) pitMap.get("SO"));
					pit.setSv((int) pitMap.get("SV"));
					pit.setW((int) pitMap.get("W"));
					pit.setWhip((Double) pitMap.get("WHIP"));
					pit.setWls((String) pitMap.get("WLS"));
					pit.setPersonFirstKname((String) pitMap.get("PERSON_FIRST_KNAME"));
					pit.setPersonFullKname((String) pitMap.get("PERSON_FULL_KNAME"));
					pit.setPersonLastKname((String) pitMap.get("PERSON_LAST_KNAME"));
					pit.setPersonId(Integer.toUnsignedLong((int) pitMap.get("PERSON_ID")));
					pit.setTeamFullName((String) pitMap.get("TEAM_FULL_NAME"));
					pit.setTeamId(Integer.toUnsignedLong((int) pitMap.get("TEAM_ID")));
					pit.setTeamName((String) pitMap.get("TEAM_NAME"));

					pitRepository.save(pit);
				}
				pitRepository.flush();
				
				List<Object> gameList = (List<Object>) entity.get("game");

				for (Object games : gameList) {

					Map<String, Object> gameMap = (Map<String, Object>) games;

					Game game;
					game = gameRepository.findOne(Integer.toUnsignedLong((int) gameMap.get("GAME_ID")));
					
					if (game == null) {
						game = new Game();
					}
					
					// map fields
					game.setAwayScore((int) gameMap.getOrDefault("AWAY_SCORE", 0));
					game.setAwaySunbalFirstKname((String) gameMap.getOrDefault("AWAY_SUNBAL_FIRST_KNAME", ""));
					game.setAwaySunbalFullKname((String) gameMap.getOrDefault("AWAY_SUNBAL_FULL_KNAME", ""));
					game.setAwaySunbalId(Integer.toUnsignedLong((int) gameMap.getOrDefault("AWAY_SUNBAL_ID", 0)));
					game.setAwaySunbalLastKname((String) gameMap.getOrDefault("AWAY_SUNBAL_LAST_KNAME", ""));
					game.setAwayTeamFullName((String) gameMap.get("AWAY_TEAM_FULL_NAME"));
					game.setAwayTeamId(Integer.toUnsignedLong((int) gameMap.get("AWAY_TEAM_ID")));
					game.setAwayTeamName((String) gameMap.get("AWAY_TEAM_NAME"));
					game.setBallparkFullName((String) gameMap.get("BALLPARK_FULL_NAME"));
					game.setBallparkId(Integer.toUnsignedLong((int) gameMap.get("BALLPARK_ID")));
					game.setBallparkName((String) gameMap.get("BALLPARK_NAME"));
					game.setBroadId((String) gameMap.get("BROAD_ID"));
					game.setBroadName((String) gameMap.get("BROAD_NAME"));
					game.setCategoryCode((String) gameMap.get("CATEGORY_CODE"));
					game.setConfirmId((String) gameMap.get("CONFIRM_ID"));
					game.setCpAwayTeamId((String) gameMap.get("CP_AWAY_TEAM_ID"));
					game.setCpGameId((String) gameMap.get("CP_GAME_ID"));
					game.setCpHomeTeamId((String) gameMap.get("CP_HOME_TEAM_ID"));
					game.setCrud((String) gameMap.get("CRUD"));
					game.setDheader((String) gameMap.get("DHEADER"));
					game.setGameDate((String) gameMap.get("GAME_DATE"));
					game.setGameDay((String) gameMap.get("GAME_DAY"));
					game.setGameId(Integer.toUnsignedLong((int) gameMap.get("GAME_ID")));
					game.setGameMonth((String) gameMap.get("GAME_MONTH"));
					game.setGameTime((String) gameMap.get("GAME_TIME"));
					game.setGameWeek((String) gameMap.get("GAME_WEEK"));
					game.setGameWeekday((String) gameMap.get("GAME_WEEKDAY"));
					game.setGameWeekDate((String) gameMap.get("GAME_WEEK_DATE"));
					game.setGrandCategoryCode((String) gameMap.get("GRAND_CATEGORY_CODE"));
					game.setHomeScore((int) gameMap.getOrDefault("HOME_SCORE", 0));
					game.setHomeSunbalFirstKname((String) gameMap.getOrDefault("HOME_SUNBAL_FIRST_KNAME", ""));
					game.setHomeSunbalFullKname((String) gameMap.getOrDefault("HOME_SUNBAL_FULL_KNAME", ""));
					game.setHomeSunbalId(Integer.toUnsignedLong((int) gameMap.getOrDefault("HOME_SUNBAL_ID", 0)));
					game.setHomeSunbalLastKname((String) gameMap.getOrDefault("HOME_SUNBAL_LAST_KNAME", ""));
					game.setHomeTeamFullName((String) gameMap.get("HOME_TEAM_FULL_NAME"));
					game.setHomeTeamId(Integer.toUnsignedLong((int) gameMap.get("HOME_TEAM_ID")));
					game.setHomeTeamName((String) gameMap.get("HOME_TEAM_NAME"));
					game.setSeasonId((String) gameMap.get("SEASON_ID"));
					game.setSeriesId(Integer.toUnsignedLong((int) gameMap.get("SERIES_ID")));
					game.setSeriesName((String) gameMap.get("SERIES_NAME"));
					game.setStatus((String) gameMap.get("STATUS"));
					game.setStReason((String) gameMap.get("ST_REASON"));
					game.setWeather((String) gameMap.get("WEATHER"));

					if (gameRepository.findOne(game.getGameId()) == null) {
						gameRepository.save(game);						
					}

				}
				gameRepository.flush();
			}
		}
	}
}
