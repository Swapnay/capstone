package com.capstone.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.capstone.model.HousePricesData;

@Repository
public interface HousingPricesRepository extends JpaRepository<HousePricesData, Long> {
    Optional<List<HousePricesData>> findByInventoryType(String inventoryType);
    Optional<List<HousePricesData>> findByState(String state);
    Optional<List<HousePricesData>> findByStateIn(Collection<String> states);

}
