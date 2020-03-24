package com.laozhang.es.base.entities;

import java.util.Date;

import com.laozhang.es.base.annotation.EntityMapping;
import com.laozhang.es.base.annotation.EntityPropertiysMapping;

@EntityMapping(Index = "productioninfo")
public class ProductionInfo extends Entity{
	@EntityPropertiysMapping(DataType="integer")
    private int devId;

	@EntityPropertiysMapping(DataType="keyword")
    private String devName;

	@EntityPropertiysMapping(DataType="keyword")
    private String devIp;

	@EntityPropertiysMapping(DataType="keyword")
    private String processingName;

	@EntityPropertiysMapping(DataType="keyword")
    private String productionLineName;

	@EntityPropertiysMapping(DataType="date")
    private Date logTime;

	@EntityPropertiysMapping(DataType="integer")
    private Integer counting;

	@EntityPropertiysMapping(DataType="integer")
    private Integer increment;

	@EntityPropertiysMapping(DataType="double")
    private Double expectIncrement;

    public int getDevId() {
        return devId;
    }

    public void setDevId(int devId) {
        this.devId = devId;
    }

    public String getDevName() {
        return devName;
    }

    public void setDevName(String devName) {
        this.devName = devName;
    }

    public String getDevIp() {
        return devIp;
    }

    public void setDevIp(String devIp) {
        this.devIp = devIp;
    }

    public String getProcessingName() {
        return processingName;
    }

    public void setProcessingName(String processingName) {
        this.processingName = processingName;
    }

    public String getProductionLineName() {
        return productionLineName;
    }

    public void setProductionLineName(String productionLineName) {
        this.productionLineName = productionLineName;
    }

    public Date getLogTime() {
        return logTime;
    }

    public void setLogTime(Date logTime) {
        this.logTime = logTime;
    }

    public Integer getCounting() {
        return counting;
    }

    public void setCounting(Integer counting) {
        this.counting = counting;
    }

    public Integer getIncrement() {
        return increment;
    }

    public void setIncrement(Integer increment) {
        this.increment = increment;
    }

    public Double getExpectIncrement() {
        return expectIncrement;
    }

    public void setExpectIncrement(Double expectIncrement) {
        this.expectIncrement = expectIncrement;
    }

    @Override
    public String toString() {
        return "ProductionInfo{" +
                "devId=" + devId +
                ", devName='" + devName + '\'' +
                ", devIp='" + devIp + '\'' +
                ", processingName='" + processingName + '\'' +
                ", productionLineName='" + productionLineName + '\'' +
                ", logTime=" + logTime +
                ", counting=" + counting +
                ", increment=" + increment +
                ", expectIncrement=" + expectIncrement +
                '}';
    }
}