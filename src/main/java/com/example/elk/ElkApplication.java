package com.example.elk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ElkApplication {

	//	niech założy repo na githubie, wrzuci tam projekt gdzie postawi sobie lokalnego ElasticSearch
	//	oraz Kafke poprzez docker-compose. Niech napisze prostą apkę w Springu - przy starcie aplikacji
	//	startuje producent, który co 10ms wysyła po Kafce update ceny produktu i doda konsumenta, który
	//	bierze te update-y, batchuje i co 5 sekund zapisuje do Elastica (to może przyjśc albo nowy produkt
	//	z ceną albo update produktu istniejącego). I niech wystawi endpoint, który pozwoli na
	//	znalezienie produktu po jego ISINie z aktualną ceną i drugi endpoint, który zwróci nam histogram
	//	cen produktów, zagregowanych do 5 bucketów


	public static void main(String[] args) {
		SpringApplication.run(ElkApplication.class, args);
	}

}
