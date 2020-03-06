/*
 * Copyright (C) 2018-2019 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * If the program is linked with libraries which are licensed under one of
 * the following licenses, the combination of the program with the linked
 * library is not considered a "derivative work" of the program:
 *
 *     - Apache License, version 2.0
 *     - Apache Software License, version 1.0
 *     - GNU Lesser General Public License, version 3
 *     - Mozilla Public License, versions 1.0, 1.1 and 2.0
 *     - Common Development and Distribution License (CDDL), version 1.0
 *
 * Therefore the distribution of the program linked with libraries licensed
 * under the aforementioned licenses, is permitted by the copyright holders
 * if the distribution is compliant with both the GNU General Public
 * License version 2 and the aforementioned licenses.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.n52.sta.service.response;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmType;

/**
 * @author <a href="mailto:s.drost@52north.org">Sebastian Drost</a>
 */
public class PropertyResponse {

    private Property property;

    private EdmType edmPropertyType;
    private EdmEntitySet responseEdmEntitySet;

    public Property getProperty() {
        return property;
    }

    public void setProperty(Property property) {
        this.property = property;
    }

    public EdmType getEdmPropertyType() {
        return edmPropertyType;
    }

    public void setEdmPropertyType(EdmType edmPropertyType) {
        this.edmPropertyType = edmPropertyType;
    }

    public EdmEntitySet getResponseEdmEntitySet() {
        return responseEdmEntitySet;
    }

    public void setResponseEdmEntitySet(EdmEntitySet responseEdmEntitySet) {
        this.responseEdmEntitySet = responseEdmEntitySet;
    }

}
