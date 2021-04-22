package com.upperlink.billerservice.repository;

import com.upperlink.billerservice.repository.predicate.CustomPredicate;
import com.upperlink.billerservice.repository.predicate.OrderBy;
import com.upperlink.billerservice.repository.predicate.PredicateBuilder;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Projections;
import org.hibernate.query.criteria.internal.OrderImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.NoResultException;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

@Repository
@Transactional
public class GenericDaoImpl {

    protected SessionFactory sessionFactory;


    @Autowired
    public GenericDaoImpl(final SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public <T> List<T> loadAllObjectsUsingRestrictions(Class<T> pObjectClass, final List<CustomPredicate> predicates, String order) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(pObjectClass);
        Root<T> root = query.from(pObjectClass);
        TypedQuery<T> typedQuery = getTypedQueryFromPredicates(builder, query, root, predicates);

        if(order != null)
            query.orderBy(builder.asc(root.get(order))); //Assuming 'order' is on the root object

        try {
            return typedQuery.getResultList();
        } catch (NoResultException ex) {
            return Collections.emptyList();
        }

    }

    public <T> int countObjectsUsingPredicateBuilder(PredicateBuilder predicateBuilder, Class<T> clazz) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<Long> query = builder.createQuery(Long.class);
        Root<T> root = query.from(clazz);
        Predicate where = builder.conjunction();
        where = predicateBuilder.build(builder, root, where);
        query.where(where);
        query.select(PredicateBuilder.getPath("id", root, Long.class));

        TypedQuery<Long> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);
        return typedQuery.getResultList().size();
    }

    public <T> T loadObjectUsingRestriction(Class<T> pObjectClass, List<CustomPredicate> predicates) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(pObjectClass);
        Root<T> root = query.from(pObjectClass);
        TypedQuery<T> typedQuery = getTypedQueryFromPredicates(builder, query, root, predicates);
        try {
            T classInstance = typedQuery.getSingleResult();
            return classInstance;
        } catch (NoResultException ex) {
            return null;
        }
    }

    public <T> T loadObjectUsingRestrictionAllowNull(Class<T> pObjectClass, List<CustomPredicate> predicates) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(pObjectClass);
        Root<T> root = query.from(pObjectClass);
        TypedQuery<T> typedQuery = getTypedQueryFromPredicates(builder, query, root, predicates);
        try {
            T classInstance = typedQuery.getSingleResult();
            return classInstance;
        } catch (NoResultException ex) {
            return null;
        }
    }

    public <T> List<T> loadPaginatedObjects(Class<T> pObjectClass, List<CustomPredicate> predicates, int pStartRowNum, int pEndRowNum,
                                            String pSortOrder, String pSortCriterion) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(pObjectClass);

        Root<T> root = query.from(pObjectClass);
        TypedQuery<T> typedQuery = getTypedQueryFromPredicates(builder, query, root, predicates);
        typedQuery.setFirstResult(pStartRowNum).setMaxResults(pEndRowNum);
        query.orderBy(builder.asc(root.get("id")));
        try {
            return typedQuery.getResultList();
        } catch (NoResultException ex) {
            return Collections.emptyList();
        }
    }

    public Long getTotalPaginatedObjects(Class<?> clazz, List<CustomPredicate> predicates) {

        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<Long> query = builder.createQuery(Long.class);
        query.select(builder.count(query.from(clazz)));
        Root<?> root = query.from(clazz);
        TypedQuery<Long> typedQuery = getTypedQueryFromPredicatesForCount(builder, query, root, clazz, predicates);
        try {
            return typedQuery.getSingleResult();
        } catch (NoResultException ex) {
            return 0l;
        }
    }

    @Transactional
    public  void storeObject(Object pObject) {
        this.sessionFactory.getCurrentSession().saveOrUpdate(pObject);
    }

    public <T> List<T> loadObjectsUsingPredicateBuilder(PredicateBuilder predicateBuilder, Class<T> clazz) {
        TypedQuery<T> typedQuery = getTypedQueryFromBuilder(predicateBuilder, clazz, Collections.EMPTY_LIST);
        try {
            return typedQuery.getResultList();
        } catch (NoResultException ex) {
            return Collections.EMPTY_LIST;
        }
    }

    public <T, X extends Number> X sumFieldUsingPredicateBuilder(Class<T> rootClass, PredicateBuilder predicateBuilder, Class<X> sumClass, String sumField,
                                                                 List<String> groupByFields) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<X> query = builder.createQuery(sumClass);
        Root<T> root = query.from(rootClass);
        Predicate where = builder.conjunction();
        where = predicateBuilder.build(builder, root, where);
        query.where(where);
        if (!groupByFields.isEmpty()) {
            List<Expression<?>> expressions = new ArrayList<>();
            for (String field: groupByFields)
                expressions.add(PredicateBuilder.getPath(field, root));
            query.groupBy(expressions);
        }
        query.select(builder.sum(PredicateBuilder.getPath(sumField, root, sumClass)));
        TypedQuery<X> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);
        return typedQuery.getSingleResult();
    }

    private <T> TypedQuery<T> getTypedQueryFromPredicates(CriteriaBuilder builder, CriteriaQuery<T> query,
                      Root<T> root, List<CustomPredicate> predicates) {
        query.select(root);
        Predicate where = builder.conjunction();

        for(CustomPredicate predicate : predicates){
            if (predicate.getField() != null)
            where = builder.and(where, builder.equal(PredicateBuilder.getPath(predicate, root), predicate.getValue()));
        }
        query.where(where);
        TypedQuery<T> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);
        return typedQuery;
    }

    private  TypedQuery<Long> getTypedQueryFromPredicatesForCount(CriteriaBuilder builder, CriteriaQuery<Long> query,
                                                          Root<?> root, Class<?> pObjectClass, List<CustomPredicate> predicates) {
        //query.select(root);
        Predicate where = builder.conjunction();
        for(CustomPredicate predicate : predicates){
            if (predicate.getField() != null)
            where = builder.and(where, builder.equal(PredicateBuilder.getPath(predicate, root), predicate.getValue()));
        }
        query.where(where);
        TypedQuery<Long> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);
        for(CustomPredicate predicate : predicates)
            typedQuery.setParameter(predicate.getField(),predicate.getValue());
        return typedQuery;
    }

    public <T> List<T> loadAllObjectsWithoutRestrictions(Class<T> pObjectClass, String order) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(pObjectClass);
        Root<T> root = query.from(pObjectClass);
        query.select(root);

        TypedQuery<T> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);

        if(order != null)
            query.orderBy(builder.asc(root.get(order))); //Assuming 'order' is on the root object
        try {
            return typedQuery.getResultList();
        } catch (NoResultException ex) {
            return Collections.EMPTY_LIST;
        }
    }

    public <T> List<T> loadObjectsUsingPredicateBuilder(PredicateBuilder predicateBuilder, Class<T> clazz, List<OrderBy> orderBy) {
        TypedQuery<T> typedQuery = getTypedQueryFromBuilder(predicateBuilder, clazz, orderBy);
        try {
            return typedQuery.getResultList();
        } catch (NoResultException ex) {
            return Collections.EMPTY_LIST;
        }
    }

    public <T> T loadSingleObjectUsingPredicateBuilder(PredicateBuilder predicateBuilder, Class<T> clazz) {
        TypedQuery<T> typedQuery = getTypedQueryFromBuilder(predicateBuilder, clazz, Collections.EMPTY_LIST);
        try {
            return typedQuery.getSingleResult();
        } catch (NoResultException ex) {
            return null;
        }
    }

    public <T> TypedQuery<T> getTypedQueryFromBuilder(PredicateBuilder predicateBuilder, Class<T> clazz, List<OrderBy> orderBy) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(clazz);
        Root<T> root = query.from(clazz);
        Predicate where = builder.conjunction();
        where = predicateBuilder.build(builder, root, where);
        query.where(where);
        if (!orderBy.isEmpty()) {
            List<Order> orders = new ArrayList<>();
            for (OrderBy order: orderBy) {
                orders.add(order.isAsc() ? new OrderImpl(PredicateBuilder.getPath(order.getField(), root)) :
                        new OrderImpl(PredicateBuilder.getPath(order.getField(), root), false));
            }
            query.orderBy(orders);
        }

        TypedQuery<T> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);
        return typedQuery;
    }

    public <T> List<T> loadAllObjectsWithSingleCondition(Class<T> pObjectClass, CustomPredicate customPredicate) {
        List<CustomPredicate> wList = new ArrayList<CustomPredicate>();
        wList.add(customPredicate);
        return this.loadAllObjectsUsingRestrictions(pObjectClass,wList,null);
    }

    public <T> List<T> loadAllObjectsWithSingleCondition(Class<T> pObjectClass, CustomPredicate customPredicate, String order) {
        List<CustomPredicate> wList = new ArrayList<CustomPredicate>();
        wList.add(customPredicate);
        return this.loadAllObjectsUsingRestrictions(pObjectClass,wList,order);
    }


    public <T> T loadObjectWithSingleCondition(Class<T> pObjectClass, CustomPredicate customPredicate) {
        List<CustomPredicate> wList = new ArrayList<CustomPredicate>();
        wList.add(customPredicate);
        return this.loadObjectUsingRestriction(pObjectClass,wList);
    }

    public <T> T loadObjectUsingKey(Class<T> pObjectClass, String key, Comparable value) {
       CustomPredicate customPredicate = new CustomPredicate(key, value);
       List<T> list = loadAllObjectsWithSingleCondition(pObjectClass, customPredicate);
       if (list != null && !list.isEmpty()) {
           return list.get(0);
       }
       return null;
    }

    public <T> T loadObjectWithSingleConditionAllowNull(Class<T> pObjectClass, CustomPredicate customPredicate) {
        List<CustomPredicate> wList = new ArrayList<>();
        wList.add(customPredicate);
        return this.loadObjectUsingRestrictionAllowNull(pObjectClass,wList);
    }

    public <T> T loadObjectById(Class<T> pObjectClass, Long pId) {

        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(pObjectClass);
        Root<T> root = query.from(pObjectClass);
        ParameterExpression<Long> parameter = builder.parameter(Long.class);
        query.select(root).where(builder.equal(root.get("id"),parameter));
        TypedQuery<T> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);
        typedQuery.setParameter(parameter,pId);
        try {
            return typedQuery.getSingleResult();
        } catch (NoResultException ex) {
            return null;
        }
    }

    public <T> List<T> loadControlEntity(Class<T> clazz) {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(clazz);
        Root<T> root = query.from(clazz);
        Field[] wFields = clazz.getDeclaredFields();

        for(Field m : wFields) {
            if(m.getName().equalsIgnoreCase("name")) {
                query.orderBy(new OrderImpl(PredicateBuilder.getPath(m.getName(), root)));
                break;
            }
        }
        TypedQuery<T> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);
        return typedQuery.getResultList();
    }

    @Transactional()
    public Long saveObject(Object object) {
        object = this.sessionFactory.getCurrentSession().merge(object);
        return (Long) this.sessionFactory.getCurrentSession().save(object);
    }

    public void storeObjectBatch(List<?> pFSaveList) {
        Transaction tx = this.sessionFactory.getCurrentSession().beginTransaction();

        for (int i = 0; i < pFSaveList.size(); i++)
        {
            this.sessionFactory.getCurrentSession().save(pFSaveList.get(i));
            if ((i % 20 != 0) && (i != pFSaveList.size() - 1)) {
                continue;
            }
            this.sessionFactory.getCurrentSession().flush();
            this.sessionFactory.getCurrentSession().clear();
        }

        tx.commit();
        this.sessionFactory.getCurrentSession().close();
    }

    public void storeVectorObjectBatch(Vector<?> pFSaveList) {
        Transaction tx = this.sessionFactory.getCurrentSession().beginTransaction();

        for (int i = 0; i < pFSaveList.size(); i++)
        {
            this.sessionFactory.getCurrentSession().save(pFSaveList.get(i));
            if ((i % 20 != 0) && (i != pFSaveList.size() - 1)) {
                continue;
            }
            this.sessionFactory.getCurrentSession().flush();
            this.sessionFactory.getCurrentSession().clear();
        }

        tx.commit();
        this.sessionFactory.getCurrentSession().close();
    }

    public void deleteObject(Object object) {
        this.sessionFactory.getCurrentSession().delete(object);
        this.sessionFactory.getCurrentSession().flush();
    }

    public <T> boolean isObjectExisting(Class<T> clazz, PredicateBuilder predicateBuilder) {
        TypedQuery<T> typedQuery = getTypedQueryFromBuilder(predicateBuilder, clazz, Collections.EMPTY_LIST);
        return !typedQuery.getResultList().isEmpty();
    }

    public Session getCurrentSession() {
        return this.sessionFactory.getCurrentSession();
    }

    public Long loadMaxValueByClassAndLongColName(Class<?> clazz, String pLongColumnOrmName) {
        List results = this.sessionFactory
                .getCurrentSession()
                .createCriteria(clazz)
                .setProjection(Projections.max(pLongColumnOrmName)).list();

        if ((results == null) || (results.size() < 1) || (results.isEmpty()) || (results.get(0) == null)) {
            return 0L;
        }
        return ((Long)results.get(0));
    }

    public <T> HashMap<Long, T> loadObjectsAsMap(Class<T> pObjectClass, List<CustomPredicate> predicates, String pMethodName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        CriteriaBuilder builder = this.sessionFactory.getCriteriaBuilder();
        CriteriaQuery<T> query = builder.createQuery(pObjectClass);
        Root<T> root = query.from(pObjectClass);
        query.select(root);
        Predicate where = builder.conjunction();
        for(CustomPredicate predicate : predicates){
            if (predicate.getField() != null)
                where = builder.and(where, builder.equal(PredicateBuilder.getPath(predicate, root), predicate.getValue()));
        }
        query.where(where);
        TypedQuery<T> typedQuery = this.sessionFactory.getCurrentSession().createQuery(query);

        List<T> classInstance = typedQuery.getResultList();

        return this.makeMap(classInstance, pMethodName);
    }

    public Long getTotalNoOfModelObject(Class<?> pObjectClass, List<CustomPredicate> predicates) {
        CriteriaBuilder cb = this.sessionFactory.getCurrentSession().getCriteriaBuilder();
        CriteriaQuery<Long> criteriaQuery = cb.createQuery(Long.class);
        Root<?> root = criteriaQuery.from(pObjectClass);
        criteriaQuery.select(cb.count(PredicateBuilder.getPath("id", root)));

        Predicate where = cb.conjunction();
        for(CustomPredicate predicate : predicates){
            if (predicate.getField() != null)
                where = cb.and(where, cb.equal(PredicateBuilder.getPath(predicate, root), predicate.getValue()));
        }
        criteriaQuery.where(where);

        TypedQuery<Long> typedQuery = this.sessionFactory.getCurrentSession().createQuery(criteriaQuery);
        return typedQuery.getSingleResult();
    }

    public Long getTotalNoOfModelObject(Class<?> pObjectClass, PredicateBuilder predicateBuilder) {
        CriteriaBuilder cb = this.sessionFactory.getCurrentSession().getCriteriaBuilder();
        CriteriaQuery<Long> criteriaQuery = cb.createQuery(Long.class);
        Root<?> root = criteriaQuery.from(pObjectClass);
        criteriaQuery.select(cb.count(PredicateBuilder.getPath("id", root)));

        Predicate where = cb.conjunction();
        where = predicateBuilder.build(cb, root, where);
        criteriaQuery.where(where);

        TypedQuery<Long> typedQuery = this.sessionFactory.getCurrentSession().createQuery(criteriaQuery);
        return typedQuery.getSingleResult();
    }

    public int getTotalNoOfModelObjectByClass(Class<?> pObjectClass,String pOrmCol ,boolean pDistinct) {
        CriteriaBuilder cb = this.sessionFactory.getCurrentSession().getCriteriaBuilder();
        CriteriaQuery<Long> criteriaQuery = cb.createQuery(Long.class);
        Root<?> root = criteriaQuery.from(pObjectClass);

        if(pDistinct)
         criteriaQuery.select(cb.countDistinct(PredicateBuilder.getPath(pOrmCol, root)));
        else
            criteriaQuery.select(cb.count(PredicateBuilder.getPath(pOrmCol, root)));

        TypedQuery<Long> typedQuery = this.sessionFactory.getCurrentSession().createQuery(criteriaQuery);
        return  ((Long)typedQuery.getSingleResult()).intValue();

    }

    private <T> HashMap<Long,T> makeMap(List<T> classInstance, String methodName) throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
        Object[] noparams = {};
        HashMap<Long,T> wMap = new HashMap<>();
        for(T t : classInstance){
            Long id = (Long) t.getClass().getMethod(methodName, null).invoke(t, noparams);
            wMap.put(id,t);
        }
        return wMap;
    }
}
