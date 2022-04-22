package in.nareshit.raghu.util;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

@Component
public class OldJwtUtil {
	
	@Value("${app.secret}")
	private String secret;
	
	public String generateToken( String subject) {
		return Jwts.builder()
				.setSubject(subject)
				.setIssuer("RAGHU")
				.setIssuedAt(new Date(System.currentTimeMillis()))
				.setExpiration(new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(30)))
				.signWith(SignatureAlgorithm.HS512, secret)
				.compact();
	}




}
