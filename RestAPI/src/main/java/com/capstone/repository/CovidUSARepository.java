package main.java.com.capstone.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.capstone.model.StateMonthlyData;


@Repository
public interface CovidUSARepository extends JpaRepository<StateMonthlyData, Long> {
    Optional<List<StateMonthlyData>> findByState(String state);
    Optional<List<StateMonthlyData>> findByStateIn(Collection<String> state);
}
