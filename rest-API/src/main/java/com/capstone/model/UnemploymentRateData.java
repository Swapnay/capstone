package  com.capstone.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotBlank;

import org.springframework.data.jpa.domain.support.AuditingEntityListener;
@Entity
@Table(name = "unemployment_monthly")
@EntityListeners(AuditingEntityListener.class)
public class UnemploymentRateData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank
    @Column(name="variable_type",nullable = false)
    private String variableType;

    @NotBlank
    @Column(name="variable_name",nullable = false)
    private String variableName;

    @NotBlank
    private int year;
    @NotBlank
    private int month;
    @NotBlank
    @Column(name="unemployed_rate",nullable = false)
    private double unemployedRate;

    @Column(name="submission_date",nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date submissionDate;

    public Long getId() {
        return id;
    }

    public double getUnemployedRate() {
        return unemployedRate;
    }

    public void setUnemployedRate(double unemployedRate) {
        this.unemployedRate = unemployedRate;
    }

    public Date getSubmissionDate() {
        return submissionDate;
    }

    public void setSubmissionDate(Date submissionDate) {
        this.submissionDate = submissionDate;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getVariableType() {
        return variableType;
    }

    public void setVariableType(String variableType) {
        this.variableType = variableType;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }


}
