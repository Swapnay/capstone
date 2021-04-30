package  com.capstone.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.capstone.model.CountryMonthlyData;


@Repository
public interface CovidWorldRepository extends JpaRepository<CountryMonthlyData, Long> {
    Optional<List<CountryMonthlyData>> findByCountry(String country);
    Optional<List<CountryMonthlyData>> findByCountryIn(Collection<String> countries);
}
