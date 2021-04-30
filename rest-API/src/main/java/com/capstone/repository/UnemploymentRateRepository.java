package  com.capstone.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.capstone.model.UnemploymentRateData;
@Repository
public interface UnemploymentRateRepository extends JpaRepository<UnemploymentRateData, Long> {
    Optional<List<UnemploymentRateData>> findByVariableType(String variableType);
    Optional<List<UnemploymentRateData>> findByVariableNameIn(Collection<String> variableNames);

}
